(ns event-sourcing.core
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder KeyValue]
           [org.apache.kafka.streams.state QueryableStoreTypes]
           [org.apache.kafka.streams.kstream ValueMapper Reducer JoinWindows ValueTransformer Transformer]

           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord])
  (:require [jackdaw.streams :as j]
            [jackdaw.client :as jc]
            [event-sourcing.flight-time-analytics :as flight-time-analytics]
            [event-sourcing.passenger-counting :as passenger-counting]
            [event-sourcing.delay-finder :as delay-finder]
            [event-sourcing.decisions :as decisions]
            [event-sourcing.query :as query]
            [event-sourcing.transducer :as transducer]
            [event-sourcing.utils :refer [topic-config]]
            [clojure.set :as set]
            [clojure.string :as str]))

; from https://www.youtube.com/watch?v=qNn2ykAaKis

; git clone https://github.com/confluentinc/cp-docker-images.git
; cd cp-docker-images/examples/kafka-single-node
; docker-compose up -d

;; OR

; git clone https://github.com/confluentinc/cp-all-in-one.git
; cd cp-all-in-one/cp-all-in-one
; git checkout 6.2.0-post
; docker-compose up          or docker-compose up -d
;
; if connecting from localhost, use localhost:9092,
;        control-center:  localhost:3030
;        schema-registry: localhost:8081
;
; if connecting from another docker container, use <container name>:<port>, such as broker:29092

;; OR

;; start kafka using one of the following:

;; On Linux or Windows(?)
; docker run --rm --net=host landoop/fast-data-dev

;; On Mac
; docker run --rm -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=localhost landoop/fast-data-dev:latest



(def app-config {"bootstrap.servers"                     "localhost:9092"
                 StreamsConfig/APPLICATION_ID_CONFIG     "aoi-state"
                 StreamsConfig/APPLICATION_SERVER_CONFIG "localhost:5051"
                 StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 500
                 ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "latest"
                 "acks"                                  "all"
                 "retries"                               "0"
                 "cache.max.bytes.buffering"             "0"})

(defn produce-one
  ([topic k v]
   (with-open [producer (jc/producer app-config (topic-config topic))]
     @(jc/produce! producer (topic-config topic) k v))))

(defonce stream-app (atom nil))
(defonce continue-monitoring? (atom true))

(defn start-topology
  ([topology]
   (start-topology topology app-config))

  ([topology app-config]
   (let [streams-builder (j/streams-builder)
         topology        (topology streams-builder)
         _               (println (-> topology j/streams-builder* .build .describe .toString))
         kafka-streams   (j/kafka-streams topology app-config)]
     (reset! stream-app kafka-streams)
     (j/start kafka-streams))))

(defn start-flex-topology
  ([topology f]
   (start-flex-topology topology f app-config))

  ([topology f app-config]
   (let [streams-builder (j/streams-builder)
         topology        (topology streams-builder f)
         _               (println (-> topology j/streams-builder* .build .describe .toString))
         kafka-streams   (j/kafka-streams topology app-config)]
     (reset! stream-app kafka-streams)
     (j/start kafka-streams))))
(defn shutdown []
  (when @stream-app
    (j/close @stream-app))
  (when @continue-monitoring?
    (reset! continue-monitoring? false)))


(defn monitor-topics
  ([topics]
   (reset! continue-monitoring? true)
   (future
     (with-open [subscription (jc/subscribed-consumer (assoc app-config "group.id" "monitor")
                                (map topic-config topics))]
       (loop [results (jc/poll subscription 200)]
         (doseq [{:keys [topic-name key value]} results]
           (println "Topic: " topic-name "\n"
             "Key:" key "\n"
             "Value:" (str/replace (with-out-str (clojure.pprint/pprint value)) #"\n" "\n       ")))
         (if @continue-monitoring?
           (recur (jc/poll subscription 200))
           nil))))))


;; Example events
(comment
  [{:flight "UA1496"}
   {:event-type :passenger-boarded
    :who        "Leslie Nielsen"
    :time       #inst "2019-03-16T00:00:00.000-00:00"
    :flight     "UA1496"}]

  [{:flight "UA1496"}
   {:event-type          :departed
    :time                #inst "2019-03-16T00:00:00.000-00:00"
    :flight              "UA1496"
    :scheduled-departure #inst "2019-03-15T00:00:00.000-00:00"}]

  [{:flight "UA1496"}
   {:event-type :arrived
    :time       #inst "2019-03-17T04:00:00.000-00:00"
    :flight     "UA1496"}]

  [{:flight "UA1496"}
   {:event-type :passenger-departed
    :who        "Leslie Nielsen"
    :time       #inst "2019-03-17T05:00:00.000-00:00"
    :flight     "UA1496"}]

  ())


;; EXAMPLE 1: Finds delayed flights from flight-events, writes to flight-status
(comment
  (do (shutdown)
      (start-topology delay-finder/find-delays-topology)
      (monitor-topics ["flight-events" "flight-status"]))

  ;; delayed departure
  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type          :departed
     :time                #inst "2019-03-16T00:00:00.000-00:00"
     :flight              "UA1496"
     :scheduled-departure #inst "2019-03-15T00:00:00.000-00:00"})

  ;; on-time departure
  (produce-one "flight-events"
    {:flight "UA1497"}
    {:event-type          :departed
     :time                #inst "2019-03-16T00:00:00.000-00:00"
     :flight              "UA1497"
     :scheduled-departure #inst "2019-03-16T00:00:00.000-00:00"})

  ())




;; EXAMPLE 2: How long is a flight in the air?
(comment
  (do (shutdown)
      (start-topology flight-time-analytics/build-time-joining-topology)
      (monitor-topics ["flight-events" "flight-times"]))

  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type          :departed
     :time                #inst "2019-03-16T00:00:00.000-00:00"
     :flight              "UA1496"
     :scheduled-departure #inst "2019-03-15T00:00:00.000-00:00"})

  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type :arrived
     :time       #inst "2019-03-16T03:00:00.000-00:00"
     :flight     "UA1496"})

  ;; region 2a) wait too long...
  (do (shutdown)
      (start-topology flight-time-analytics/build-table-joining-topology)
      (monitor-topics ["flight-events" "flight-times"]))

  (produce-one "flight-events"
    {:flight "UA1497"}
    {:event-type          :departed
     :time                #inst "2019-03-16T00:00:00.000-00:00"
     :flight              "UA1497"
     :scheduled-departure #inst "2019-03-15T00:00:00.000-00:00"})

  (produce-one "flight-events"
    {:flight "UA1497"}
    {:event-type :arrived
     :time       #inst "2019-03-16T04:00:00.000-00:00"
     :flight     "UA1497"})
  ;; endregion
  ())



;; EXAMPLE 3: Who is on the plane?
(comment
  (do (shutdown)
      (start-topology passenger-counting/build-boarded-counting-topology)
      (monitor-topics ["flight-events" "passenger-set"]))

  ;; Leslie Nielsen boarded
  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type :passenger-boarded
     :who        "Leslie Nielsen"
     :time       #inst "2019-03-16T00:00:00.000-00:00"
     :flight     "UA1496"})

  (query/get-passengers @stream-app "UA1496")

  ;; Leslie Nielsen Departed
  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type :passenger-departed
     :who        "Leslie Nielsen"
     :time       #inst "2019-03-16T00:00:00.000-00:00"
     :flight     "UA1496"})


  (query/get-passengers @stream-app "UA1496")

  ;; Julie Hagerty boarded
  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type :passenger-boarded
     :who        "Julie Hagerty"
     :time       #inst "2019-03-16T00:00:00.000-00:00"
     :flight     "UA1496"})

  ;; Julie Hagerty departed
  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type :passenger-departed
     :who        "Julie Hagerty"
     :time       #inst "2019-03-16T00:00:00.000-00:00"
     :flight     "UA1496"})

  ())


;; EXAMPLE 3A: can we do something similar with AoIs?
(comment
  (do (shutdown)
      (start-flex-topology
        passenger-counting/build-aoi-state-topology
        passenger-counting/aoi->aoi-state-replace)
      (monitor-topics ["aois" "aoi-state"]))

  (do (shutdown)
    (start-flex-topology
      passenger-counting/build-aoi-state-topology
      passenger-counting/aoi->aoi-state-aggregated)
    (monitor-topics ["aois" "aoi-state"]))

  ;; add an aoi
  (produce-one "aois"
    {:aoi "alpha"}
    {:event-type :aoi-added
     :aoi-needs  #{[7 7 "hidef" 0]}
     :aoi        "alpha"})

  (query/get-one-aoi @stream-app "aoi-state" "alpha")
  (query/get-one-aoi @stream-app "aoi-state" "bravo")

  (query/get-all-aois @stream-app)
  (query/get-all-values @stream-app "aoi-state")

  (->> "aoi-state"
    (query/get-all-values @stream-app)
    (map (fn [[k v]]
           {(:aoi k) v}))
    (into {}))

  {:id "bravo"
   :data-set (query/get-one-aoi @stream-app "aoi-state" "bravo")}



  (produce-one "aois"
    {:aoi "bravo"}
    {:event-type :aoi-added
     :aoi-needs  #{[9 9 "hidef" 0]}
     :aoi        "bravo"})
  (produce-one "aois"
    {:aoi "bravo"}
    {:event-type :aoi-added
     :aoi-needs  #{[9 9 "hidef" 0] [3 3 "hidef" 0]}
     :aoi        "bravo"})

  (produce-one "aois"
    {:aoi "alpha"}
    {:event-type :aoi-added
     :aoi-needs  #{[7 6 "hidef" 1]}
     :aoi        "alpha"})
  (produce-one "aois"
    {:aoi "alpha"}
    {:event-type :aoi-added
     :aoi-needs  #{[7 5 "hidef" 2]}
     :aoi        "alpha"})

  (produce-one "aois"
    {:aoi "alpha"}
    {:event-type :aoi-removed
     :aoi-needs  #{[7 6 "hidef" 1]}
     :aoi        "alpha"})
  (produce-one "aois"
    {:aoi "alpha"}
    {:event-type :aoi-added
     :aoi-needs  #{[7 4 "hidef" 3]}
     :aoi        "alpha"})


  (produce-one "aois"
    {:aoi "alpha"}
    {:event-type :aoi-deleted
     :aoi        "alpha"})



  ())



;; EXAMPLE 3B: how do we use the same KTable from 2 different apps?
(comment
  (do (shutdown)
      (start-flex-topology
        passenger-counting/build-state-topology
        passenger-counting/aoi-state)
      (monitor-topics ["aoi-state"]))

  (query/get-one-aoi @stream-app "alpha")
  (query/get-one-aoi @stream-app "bravo")

  (query/get-all-aois @stream-app)
  (query/get-all-values @stream-app "aoi-state")

  (->> "aoi-state"
    (query/get-all-values @stream-app)
    (map (fn [[k v]]
           {(:aoi k) v}))
    (into {}))


  (produce-one "aois"
    {:aoi "bravo"}
    {:event-type :aoi-added
     :aoi-needs  [9 9 "hidef" 0]
     :aoi        "bravo"})
  (produce-one "aois"
    {:aoi "bravo"}
    {:event-type :aoi-added
     :aoi-needs  [3 3 "hidef" 0]
     :aoi        "bravo"})

  (produce-one "aois"
    {:aoi "alpha"}
    {:event-type :aoi-added
     :aoi-needs  [7 6 "hidef" 1]
     :aoi        "alpha"})
  (produce-one "aois"
    {:aoi "alpha"}
    {:event-type :aoi-added
     :aoi-needs  [7 5 "hidef" 2]
     :aoi        "alpha"})

  (produce-one "aois"
    {:aoi "alpha"}
    {:event-type :aoi-removed
     :aoi-needs  [7 6 "hidef" 1]
     :aoi        "alpha"})
  (produce-one "aois"
    {:aoi "alpha"}
    {:event-type :aoi-added
     :aoi-needs  [7 4 "hidef" 3]
     :aoi        "alpha"})


  (produce-one "aois"
    {:aoi "alpha"}
    {:event-type :aoi-deleted
     :aoi        "alpha"})



  ())




;; EXAMPLE 4: Count passengers as they board the plane
(comment
  (do (shutdown)
      (start-topology passenger-counting/build-boarded-decorating-topology)
      (monitor-topics ["flight-events" "flight-events-with-passengers"]))

  ;; Robert Hays boarded
  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type :passenger-boarded
     :who        "Robert Hays"
     :time       #inst "2019-03-16T00:00:00.000-00:00"
     :flight     "UA1496"})

  (do (shutdown)
      (start-topology passenger-counting/build-boarded-decorating-topology-cleaner)
      (monitor-topics ["flight-events" "flight-events-with-passengers"]))

  ;; Julie Hagerty boarded
  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type :passenger-boarded
     :who        "Julie Hagerty"
     :time       #inst "2019-03-16T00:00:00.000-00:00"
     :flight     "UA1496"})


  (query/get-passengers @stream-app "UA1496")

  ())


;; EXAMPLE 5: Are my friends on the plane?
(comment

  (query/get-passengers @stream-app "UA1496")
  (query/friends-onboard? @stream-app "UA1496"
    #{"Leslie Nielsen" "Julie Hagerty" "Peter Graves"})

  (query/friends-onboard-cleaner? @stream-app "UA1496"
    #{"Leslie Nielsen" "Julie Hagerty" "Peter Graves"})

  (query/friends-raw? @stream-app)

  ())




;; EXAMPLE 6: Clean the plane when the last passenger departs
(comment

  (do (shutdown)
      (start-topology decisions/build-clean-plane-topology)
      (monitor-topics ["flight-events" "flight-decisions"]))

  ;; Leslie Nielsen Departed
  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type :passenger-boarded
     :who        "Leslie Nielsen"
     :time       #inst "2019-03-16T00:00:00.000-00:00"
     :flight     "UA1496"})

  ;; Leslie Nielsen Departed
  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type :passenger-departed
     :who        "Leslie Nielsen"
     :time       #inst "2019-03-16T00:00:00.000-00:00"
     :flight     "UA1496"})
  ;; Robert Hays Departed
  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type :passenger-departed
     :who        "Robert Hays"
     :time       #inst "2019-03-16T00:00:00.000-00:00"
     :flight     "UA1496"})

  ;; Julie Hagerty Departed
  (produce-one "flight-events"
    {:flight "UA1496"}
    {:event-type :passenger-departed
     :who        "Julie Hagerty"
     :time       #inst "2019-03-16T00:00:00.000-00:00"
     :flight     "UA1496"})


  (query/get-passengers @stream-app "UA1496")


  ())



;; EXAMPLE 7: Fixing a bug
(comment
  (do (shutdown)
      (start-topology decisions/build-clean-plane-topology
        (assoc app-config
          StreamsConfig/APPLICATION_ID_CONFIG "cleaning-planner-bugfix"
          ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"))

      (monitor-topics ["flight-events" "flight-decisions"])))


;; EXAMPLE 8: Transducers
(comment
  (do (shutdown)
      (start-topology transducer/build-transducer-topology)
      (monitor-topics ["flight-events" "transduced-events"])))



;; EXAMPLE 9: word count...
(comment
  (do (shutdown)
      (start-topology passenger-counting/word-count)
      (monitor-topics ["streams-plaintext-input" "streams-wordcount-output"]))

  ;; add an aoi
  (produce-one "streams-plaintext-input"
    {:aoi "alpha"}
    {:val "here IS a String to test"})

  (defn get-count [streams ktable-name]
    (let [s (-> streams
              (.store ktable-name (QueryableStoreTypes/keyValueStore)))
          k (.all s)]
      (into {}
        (map (fn [x]
               {(.key x) (.value x)})
          (iterator-seq k)))))


  (get-count @stream-app "counts-store")


  ())


(comment
  (shutdown))



