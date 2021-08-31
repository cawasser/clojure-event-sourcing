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


(defn get-one-aoi [streams aoi]
  (-> streams
    (.store "aoi-status" (QueryableStoreTypes/keyValueStore))
    (.get {:aoi aoi})))


(defn get-all-aois [streams]
  (let [s (-> streams
            (.store "aoi-status" (QueryableStoreTypes/keyValueStore)))
        k (.all s)]
    (map #(.get {:aoi %} s) k)))



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
