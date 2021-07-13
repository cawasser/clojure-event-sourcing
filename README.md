# Event-sourced systems with Kafka, Clojure, and Jackdaw

This repository is part of a Clojure meetup talk, walking through building event-sourced systems with Kafka. 



## Slides

You can walk through the slides [here](https://www.slideshare.net/BryceCovert1/eventsourced-systems-with-kafka-clojure-and-jackdaw).

## Try it out

You'll first want to set up kafka on your local machine.
```
git clone https://github.com:confluentinc/cp-docker-images.git
cd cp-docker-images/examples/kafka-single-node
docker-compose up -d
```

OR

```
git clone https://github.com/confluentinc/cp-all-in-one.git
cd cp-all-in-one/cp-all-in-one
git checkout 6.2.0-post
docker-compose up -d
```

OR

use [Landoop Kafka](https://www.mrjamiebowman.com/software-development/getting-started-with-landoop-kafka-on-docker-locally/)



Next, fire up a REPL and walk through the commented sections in core.clj.



## License

Copyright © 2019 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
