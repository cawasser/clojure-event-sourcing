(ns event-sourcing.transducer
  (:require [event-sourcing.utils :refer [topic-config]]
    [jackdaw.client :as jc]
    [jackdaw.serdes.edn :as jse]
    [jackdaw.streams :as j]))



(defn transformer-supplier [xform]
  #(let [processor-context (atom nil)]
     (reify Transformer
       (init [_ pc]
         (reset! processor-context pc))
       (transform [_ k v]
         (transduce xform
           (fn ([processor-context [k v]]
                (.forward processor-context k v)
                processor-context)
             ([processor-context]
              processor-context))
           @processor-context
           [[k v]])
         (.commit @processor-context)
         nil)
       (close [_]))))



(defn build-transducer-topology [builder]
  (let [xform (comp (filter (fn [[k v]]
                              (#{:passenger-boarded :passenger-departed} (:event-type v))))
                (map (fn [[k v]]
                       [k (assoc v :decorated? true)]))
                (mapcat (fn [[k v]]
                          [[k v]
                           [k v]])))
        boarded-events (-> builder
                         (j/kstream (topic-config "flight-events"))
                         (j/transform (transformer-supplier xform))
                         (j/to (topic-config "transduced-events")))])
  builder)



(comment
  (def messages [[{:flight "UA1496"}
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
                   :flight     "UA1496"}]])


  (->> messages
    (filter (fn [[k v]]
              (#{:passenger-boarded :passenger-departed} (:event-type v))))
    (map (fn [[k v]]
           [k (assoc v :decorated? true)]))
    (mapcat (fn [[k v]]
              [[k v]
               [k v]])))

  (def xd (comp (filter (fn [[k v]]
                          (#{:passenger-boarded :passenger-departed} (:event-type v))))
            (map (fn [[k v]]
                   [k (assoc v :decorated? true)]))
            (mapcat (fn [[k v]]
                      [[k v]
                       [k v]]))))


  ())






