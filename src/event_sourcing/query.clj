(ns event-sourcing.query
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder KeyValue]
           [org.apache.kafka.streams.state QueryableStoreTypes]
           [org.apache.kafka.streams.kstream ValueMapper Reducer JoinWindows ValueTransformer Transformer]

           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:require [jackdaw.streams :as j]
            [event-sourcing.utils :refer [topic-config]]))

(defn get-passengers [streams flight]
  (-> streams
    (.store "passenger-set" (QueryableStoreTypes/keyValueStore))
    (.get {:flight flight})))


(defn get-one-aoi [streams store aoi]
  (-> streams
    (.store store (QueryableStoreTypes/keyValueStore))
    (.get {:aoi aoi})))


(defn get-all-aois [streams]
  (let [s (-> streams
            (.store "aoi-state" (QueryableStoreTypes/keyValueStore)))
        k (.all s)]
    (into {}
      (map (fn [x]
             {(.key x) (.value x)})
        (iterator-seq k)))))


(defn get-all-values [streams ktable-name]
  (let [s (-> streams
            (.store ktable-name (QueryableStoreTypes/keyValueStore)))
        k (.all s)]
    (into {}
      (map (fn [x]
             {(.key x) (.value x)})
        (iterator-seq k)))))


(defn friends-onboard? [streams flight friends]
  (-> streams
    (.store "passenger-set" (QueryableStoreTypes/keyValueStore))
    (.get {:flight flight})
    (clojure.set/intersection friends)))


(defn friends-onboard-cleaner? [streams flight friends]
  (-> (get-passengers streams flight)
    (clojure.set/intersection friends)))


(defn friends-raw? [streams]
  (-> streams
    (.store "passenger-set" (QueryableStoreTypes/keyValueStore))))



(comment

  (def ktable-data {{:aoi "alpha"} #{[7 7 "hidef" 0]},
                    {:aoi "bravo"} #{[9 9 "hidef" 0]}})

  (first ktable-data)

  (map (fn [[{aoi :aoi} v]]
         {:id aoi :data-set v})
   ktable-data)


  ())