### Running Beam Mobile Gaming Examples

* Beam Mobile Gaming Examples are documented here: https://beam.apache.org/get-started/mobile-gaming-example/
* Java implementations are here: https://github.com/apache/beam/tree/master/examples
*  Clojure/thurber ports of each example can be found in [../demo/game](../demo/game).

Google Cloud provides free trials so these examples can be executed in Dataflow
 without cost.

#### Prerequisites

Before attempting to execute thurber demos in Dataflow:

**1/** Create `thurber-demo` GCP project.

Alternatively you can use your default project or any other project you create;
just replace references to `thurber-demo` project in this documentation with the
project you choose to use.

**2/** Create `gs://thurber-demo/` bucket in GCS (Google Cloud Storage) in
the project you intend to execute the examples.

**3/** Enable Dataflow and Dataflow API in GCP.

(If you don't know how to do this, you'll get an error trying to deploy
these demos which will give you a link to do this.)

**4/** Setup a GCP Service Account

You'll need to 
[setup a service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts)
to run these examples. 

This involves ensuring a `GOOGLE_APPLICATION_CREDENTIALS` environment var is
set before deploying any example.

**5/** Build demo uberjar

To run any of these examples in Dataflow you'll need to build a deployable uberjar
containing the code.

    lein with-profile +demo,+dataflow uberjar

Note: the resultant JAR contains our non-AOT'd Clojure sources that will dynamically
compile and execute in Dataflow!

### User Score

Follow Prerequisites above.

You may need to adjust `--project` and `--region` parameters in accordance
with your GCP account:

    lein with-profile +demo,+dataflow run -m game.user-score/demo! \
          --appName="thurber-demo-user-score" \
          --jobName="thurber-demo-user-score-$(date +%s)" \
          --runner=DataflowRunner \
          --region=us-central1 --project=thurber-demo \
          --gcpTempLocation=gs://thurber-demo/gcp-temp \
          --filesToStage=$(ls target/*-standalone.jar)
          
### Hourly Team Score

    lein with-profile +demo,+dataflow run -m game.hourly-team-score/demo! \
      --appName="thurber-demo-hourly-team-score" \
      --jobName="thurber-demo-hourly-team-score-$(date +%s)" \
      --runner=DataflowRunner \
      --region=us-central1 --project=thurber-demo \
      --gcpTempLocation=gs://thurber-demo/gcp-temp \
      --filesToStage=$(ls target/*-standalone.jar)

### Leaderboard

* Follow Prerequisites above
* Create topic
    * `gcloud pubsub topics create --project thurber-demo thurber-demo-game`
* Create dataset and table
    * `bq mk --project_id thurber-demo --dataset thurber_demo_game`
    * `bq mk --project_id thurber-demo --table thurber_demo_game.leaderboard`


    
    lein with-profile +demo,+dataflow run -m game.leader-board/demo! \
      --appName="thurber-demo-leaderboard" \
      --jobName="thurber-demo-leaderboard-$(date +%s)" \
      --runner=DataflowRunner \
      --region=us-central1 --project=thurber-demo \
      --gcpTempLocation=gs://thurber-demo/gcp-temp \
      --filesToStage=$(ls target/*-standalone.jar)
      
In a separate terminal window, generate streaming data:

    lein with-profile +demo,+dataflow run -m game.injector \
        thurber-demo thurber-demo-game none
        
* Afterwards, clean up
    * `gcloud pubsub topics delete --project thurber-demo thurber-demo-game`
    * `bq rm --project_id thurber-demo --table thurber_demo_game.leaderboard`
    * `bq rm --project_id thurber-demo --dataset thurber_demo_game`

### Game Stats

todo 

### Stateful Team Score

todo