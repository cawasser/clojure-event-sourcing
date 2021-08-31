(ns event-sourcing.passenger-counting
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder KeyValue]
           [org.apache.kafka.streams.state QueryableStoreTypes]
           [org.apache.kafka.streams.kstream ValueMapper Reducer JoinWindows ValueTransformer Transformer]

           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:require [jackdaw.streams :as j]
            [event-sourcing.utils :refer [topic-config]]))

(defn flight->passenger-count-ktable [flight-events-stream]
  (-> flight-events-stream
    (j/filter (fn [[k v]]
                (#{:passenger-boarded :passenger-departed} (:event-type v))))
    (j/group-by-key)
    (j/aggregate (constantly #{})
      (fn [current-passengers [_ event]]
        (cond-> current-passengers
         (= :passenger-boarded (:event-type event)) (conj (:who event))
         (= :passenger-departed (:event-type event)) (disj (:who event))))
      (topic-config "passenger-set"))))

(defn build-boarded-counting-topology [builder]
  (let [flight-events-stream (j/kstream builder (topic-config "flight-events"))]
    (-> flight-events-stream
      (flight->passenger-count-ktable)
      (j/to-kstream)
      ;(j/map (fn [[k passengers]]
      ;         [k (assoc k :passengers passengers)]))
      (j/to (topic-config "passenger-set"))))
  builder)


(defn aoi->aoi-status [aoi-events-stream]
  (-> aoi-events-stream
    (j/filter (fn [[k v]]
                (#{:aoi-added :aoi-removed :aoi-deleted} (:event-type v))))
    (j/group-by-key)
    (j/aggregate (constantly #{})
      (fn [aoi-state [_ event]]
        (prn aoi-state (:aoi-needs event))
        (cond-> aoi-state
          (= :aoi-added (:event-type event)) (conj (:aoi-needs event))
          (= :aoi-removed (:event-type event)) (disj (:aoi-needs event))
          (= :aoi-deleted (:event-type event)) #{}))
      (topic-config "aoi-status"))))


(defn build-aoi-status-topology [builder]
  (let [aoi-events-stream (j/kstream builder (topic-config "aois"))]
    (-> aoi-events-stream
      (aoi->aoi-status)
      (j/to-kstream)
      (j/to (topic-config "aoi-status"))))
  builder)




(defn build-boarded-decorating-topology [builder]
  (let [flight-events-stream (j/kstream builder (topic-config "flight-events"))
        passengers-ktable    (flight->passenger-count-ktable flight-events-stream)
        passenger-store-name (.queryableStoreName (j/ktable* passengers-ktable))]
    (-> flight-events-stream
      (j/transform-values #(let [passenger-store (atom nil)]
                             (reify ValueTransformer
                               (init [_ pc]
                                 (reset! passenger-store (.getStateStore pc passenger-store-name)))
                               (transform [_ v]
                                 (assoc v :passenger-count (count (.get @passenger-store {:flight (:flight v)}))))
                               (close [_])))
        [passenger-store-name])
      (j/to (topic-config "flight-events-with-passengers")))
    builder))

(defn transform-with-stores [stream f store-names]
  (j/transform-values stream #(let [stores (atom nil)]
                                (reify ValueTransformer
                                  (init [_ pc]
                                    (reset! stores (mapv (fn [s] (.getStateStore pc s)) store-names)))
                                  (transform [_ v]
                                    (f v @stores))
                                  (close [_])))
    store-names))

(defn build-boarded-decorating-topology-cleaner [builder]
  (let [flight-events-stream (j/kstream builder (topic-config "flight-events"))
        passengers-ktable    (flight->passenger-count-ktable flight-events-stream)
        passenger-store-name (.queryableStoreName (j/ktable* passengers-ktable))]
    (-> flight-events-stream
      (transform-with-stores (fn [event [passenger-store]]
                               (assoc event :passenger-count (count (.get passenger-store {:flight (:flight event)}))))
        [passenger-store-name])
      (j/to (topic-config "flight-events-with-passengers")))
    builder))




