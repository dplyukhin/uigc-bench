package randomworkers
import com.typesafe.config.ConfigFactory

class Config() {
  private val config           = ConfigFactory.load("random-workers")
  val acyclic                  = config.getBoolean("random-workers.acyclic")
  val jvmGCFrequency           = config.getInt("random-workers.jvm-gc-frequency")
  val reqsPerSecond            = config.getInt("random-workers.reqs-per-second")
  val maxWorkSizeBytes         = config.getInt("random-workers.max-work-size-in-bytes")
  val maxAcqsInOneMsg          = config.getInt("random-workers.max-acqs-per-msg")
  val maxSendsInOneTurn        = config.getInt("random-workers.max-sends-per-turn")
  val maxSpawnsInOneTurn       = config.getInt("random-workers.max-spawns-per-turn")
  val maxDeactivatedInOneTurn  = config.getInt("random-workers.max-deactivated-per-turn")
  val managerMaxAcquaintances  = config.getInt("random-workers.manager-max-acquaintances")
  val totalQueries             = config.getInt("random-workers.total-queries")
  val queryTimesFile           = config.getString("random-workers.query-times-file")
  val lifeTimesFile            = config.getString("random-workers.life-times-file")
  val managerProbSpawn         = config.getDouble("random-workers.mgr-probabilities.spawn")
  val managerProbLocalSend     = config.getDouble("random-workers.mgr-probabilities.local-send")
  val managerProbRemoteSend    = config.getDouble("random-workers.mgr-probabilities.remote-send")
  val managerProbLocalAcquaint = config.getDouble("random-workers.mgr-probabilities.local-acquaint")
  val managerProbRemoteAcquaint =
    config.getDouble("random-workers.mgr-probabilities.remote-acquaint")
  val managerProbPublishWorker = config.getDouble("random-workers.mgr-probabilities.publish-worker")
  val managerProbDeactivate    = config.getDouble("random-workers.mgr-probabilities.deactivate")
  val managerProbDeactivateAll = config.getDouble("random-workers.mgr-probabilities.deactivate-all")
  val managerProbQuery         = config.getDouble("random-workers.mgr-probabilities.query")
  val workerProbSpawn          = config.getDouble("random-workers.wrk-probabilities.spawn")
  val workerProbSend           = config.getDouble("random-workers.wrk-probabilities.send")
  val workerProbAcquaint       = config.getDouble("random-workers.wrk-probabilities.acquaint")
  val workerProbDeactivate     = config.getDouble("random-workers.wrk-probabilities.deactivate")
  val workerProbDeactivateAll  = config.getDouble("random-workers.wrk-probabilities.deactivate-all")
}
