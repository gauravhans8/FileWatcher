import java.nio.file._
import scala.collection._
import scala.collection.mutable._
import java.io._
import java.net._
import java.io.FileInputStream
import java.security.MessageDigest
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.concurrent.{Future, future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import sun.misc.Signal
import sun.misc.SignalHandler
import java.util.Calendar
import scala.concurrent.duration._

object FileWatcher {

  var directoryPath: String = ""
  var logfileString: String = ""
  var listFile = new ListBuffer[String]()
  val sc = new SparkContext(new SparkConf().setAppName("FileWatcher").setMaster("local[*]"))
  var hashmap: mutable.Map[String, List[String]] = mutable.Map[String, List[String]]()

  def main(args: Array[String]): Unit = {

    var logfilePath: String = ""
    if (args.length == 0){
      println("No directory specified, exiting")
      sys.exit(0)
    }
    else if (args.length < 2)
      logfilePath = sys.env("HOME") + "/logfile.txt"
    else
      logfilePath = args(1)
    //logfilePath = sys.env("HOME") + "/logfile.txt"
    directoryPath = args(0)
    hashmap = createHashMap(directoryPath)
    val fWrite = new FileWriter(logfilePath, true)
    val hadoopConf = new Configuration()
    val dirName = directoryPath.split("/")
    val hdfs = FileSystem.get(new URI("hdfs://localhost:8020"), hadoopConf)
    hdfs.mkdirs(new Path("/user/file_watcher/" + dirName(dirName.length-1)))
    hdfs.close()
    val dirPath = Paths.get(directoryPath)
    val watchService = dirPath.getFileSystem.newWatchService()
    dirPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE)

    while (true) {
      val key = watchService.take()
      for (event <- key.pollEvents().asScala) {
        //println(someFile.length)
        if (!event.kind.equals(StandardWatchEventKinds.OVERFLOW)) {
          val someFile = new File(directoryPath + event.context().toString)
          //println(someFile.length)
          var newSize: Integer = 0
          var prevSize = newSize
          do {
            Thread.sleep(2000)
            prevSize = newSize
            newSize = someFile.length.toInt
          } while (prevSize != newSize)
          if (!event.context().toString.matches(".*\\.tlr$") && !event.context().toString.matches("\\..*")) {
            if (event.kind.equals(StandardWatchEventKinds.ENTRY_CREATE)) {
              if (listFile.contains(event.context().toString)) {
                Await.ready(deleteFile(event.context().toString, flag = false), 2 second)
                //Thread.sleep(1000)
              }
              if (Files.probeContentType(Paths.get(directoryPath + event.context().toString)).equals("text/plain")) {
                //print(hashValue)
                val hashValue = getHashValue(event.context().toString)
                if (hashmap.contains(hashValue)) {
                  filecopy(event.context().toString, hashValue, flag = true)
                }
                else {
                  val thread = new Thread {
                    override def run() {
                      createTlr(event.context().toString, hashValue, flag = true)
                      // your custom behavior here
                    }
                  }
                  thread.start()
                }
              }
              else {
                logfileString = logfileString.concat(Calendar.getInstance.getTime.toString + "\t" + event.context().toString + " is not a text file \n")
              }
            }
            else if (event.kind.equals(StandardWatchEventKinds.ENTRY_DELETE)) {
              //println("This Happened")
              listFile.remove(listFile.indexOf(event.context().toString))
              deleteFile(event.context().toString, flag = true).onComplete {
                case Success(e) => println("Deleted " + event.context().toString)
                case Failure(e) => e.printStackTrace()
              }
            }
          }
        }
        else {
          handleOverflow.onComplete {
            case Success(e) => println("okay")
            case Failure(e) => e.printStackTrace()
          }
        }
        //println(logfileString)
        fWrite.write(logfileString)
        fWrite.flush()
        logfileString = ""
      }
      key.reset()
    }
  }

  def getHashValue(filename: String): String = {
    val inputStream = new FileInputStream(directoryPath + filename)
    //println(Calendar.getInstance.getTime.toString)
    val digest = MessageDigest.getInstance("SHA-1")
    val bytesBuffer = new Array[Byte](8192)
    var bytesRead: Int = -1
    bytesRead = inputStream.read(bytesBuffer)
    while (bytesRead != -1) {
      digest.update(bytesBuffer, 0, bytesRead)
      bytesRead = inputStream.read(bytesBuffer)
    }
    val hashedBytes = digest.digest
    //println(Calendar.getInstance.getTime.toString)
    inputStream.close()
    hashedBytes.map("%02X" format _).mkString
  }

  def filecopy(filename: String, hashValue: String, flag: Boolean): Unit = {
    val copyfile = hashmap(hashValue).head
    val sourcePath = Paths.get(directoryPath + copyfile + ".tlr")
    val destinationPath = Paths.get(directoryPath + filename + ".tlr")
    Files.copy(sourcePath, destinationPath)
    if (flag) {
      logfileString = logfileString.concat(Calendar.getInstance.getTime.toString + "\tHash value already present for " + filename + "\n")
      logfileString = logfileString.concat(Calendar.getInstance.getTime.toString + "\tCopying trailer file for " + filename + " from " + copyfile + "\n")
      var list1 = hashmap(hashValue).to[ListBuffer].clone
      list1 += filename
      hashmap += (hashValue -> list1.toList)
      listFile += filename
      //println(s"$hashmap")
      hadoopBackUp(filename)
    }
  }

  def createTlr(filename: String, hashValue: String, flag: Boolean): Future[Unit] = future {
    //println(logfileString)
    val texFile = sc.textFile(directoryPath + filename).filter(!_.isEmpty)
    val count1 = texFile.count().toInt
    val wordCounts = texFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).cache()
    val temp1 = wordCounts.map(_.swap)
    val temp2 = temp1.sortByKey(false, 1)
    val temp3 = temp2.take(1).mkString(" ")
    val name = directoryPath + filename + ".tlr"
    val fw = new FileWriter(name, true)
    fw.write((if (temp3.isEmpty) "Empty File" else temp3) + "\n" + count1.toString + "\n" + hashValue)
    fw.close()
    if (flag) {
      logfileString = logfileString.concat(Calendar.getInstance.getTime.toString + "\tCreated trailer file for " + filename + "\n")
      val list = List(filename)
      hashmap += (hashValue -> list)
      listFile += filename
      //println(s"$hashmap")
      hadoopBackUp(filename)
    }
  }

  def hadoopBackUp(filename: String): Unit = {
    logfileString = logfileString.concat(Calendar.getInstance.getTime.toString + "\tBacking up both the files on hdfs -> " + filename + "\n")
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(new URI("hdfs://localhost:8020"), hadoopConf)
    var srcPath = new Path(directoryPath + filename)
    val dirName = directoryPath.split("/")
    val destPath = new Path("hdfs://localhost:8020/user/file_watcher/" + dirName(dirName.length-1))
    hdfs.copyFromLocalFile(srcPath, destPath)
    srcPath = new Path(directoryPath + filename + ".tlr")
    hdfs.copyFromLocalFile(srcPath, destPath)
    hdfs.close()
  }

  def handleOverflow: Future[Unit] = future {
    logfileString = logfileString.concat(Calendar.getInstance.getTime.toString + "\tOverflow Event\n")
    var listTemp: ListBuffer[String] = new ListBuffer[String]
    val listofFiles = new File(directoryPath).listFiles()
    //println(listofFiles.size)
    logfileString = logfileString.concat(Calendar.getInstance.getTime.toString + "\tRunning a parallel thread for the current batch size: " + listofFiles.size + "\n")
    for (file <- listofFiles) {
      if (file.isFile) {
        listTemp += file.getName
      }
    }
    for (filename <- listTemp) {
      if (Files.probeContentType(Paths.get(directoryPath + filename)).equals("text/plain")) {
        if (!filename.matches(".*.tlr$") && !filename.matches(".*~$")) {
          val hashValue = getHashValue(filename)
          if (hashmap.contains(hashValue)) {
            if (!Files.exists(Paths.get(directoryPath + filename + ".tlr"))) filecopy(filename, hashValue, flag = true)
          }
          else {
            if (!Files.exists(Paths.get(directoryPath + filename + ".tlr"))) createTlr(filename, hashValue, flag = true)
          }
        }
      }
      else{
        logfileString = logfileString.concat(Calendar.getInstance.getTime.toString + "\t" + filename + " is not a text file \n")
      }
    }
  }

  def deleteFile(filename: String, flag: Boolean): Future[Unit] = future {
    if (flag) {
      Thread.sleep(10000)
      logfileString = logfileString.concat(Calendar.getInstance.getTime.toString + "\tFile Deleted -> " + filename + "\n")
    }
    try {
      val deletePath = Paths.get(directoryPath + filename + ".tlr")
      val hashValue = Files.readAllLines(deletePath).get(2)
      Files.delete(deletePath)
      if (flag)
        logfileString = logfileString.concat(Calendar.getInstance.getTime.toString + "\tTrailer File also deleted for " + filename + "\n")
      var list1 = hashmap(hashValue).to[ListBuffer].clone
      list1.remove(list1.indexOf(filename))
      if(listFile.contains("filename"))
        listFile.remove(listFile.indexOf(filename))
      if (list1.isEmpty)
        hashmap.remove(hashValue)
      else
        hashmap += (hashValue -> list1.toList)
      //println(hashmap)
    }
    catch {
      case e: NoSuchFileException => println()
    }
  }

  Signal.handle(new Signal("INT"), new SignalHandler() {
    def handle(sig: Signal) {
      sc.stop()
      sys.exit(0)
    }
  })

  def createHashMap(directory: String): mutable.Map[String, List[String]] = {
    val filelist = mutable.Map[String, List[String]]()
    var listTemp = new ListBuffer[String]
    var listTempFile = new ListBuffer[String]
    val listofFiles = new File(directory).listFiles()
    for (file <- listofFiles) {
      if (file.isFile) {
        listTemp += file.getName
      }
    }
    for (filename <- listTemp) {
      if (!filename.matches(".*\\.tlr$") && !filename.matches(".*~$")) {
        listTempFile += filename
        val hashValue = getHashValue(filename)
        if (filelist.contains(hashValue)) {
          if (!Files.exists(Paths.get(directoryPath + filename + ".tlr"))) filecopy(filename, hashValue, flag = false)
          var list1 = filelist(hashValue).to[ListBuffer].clone
          list1 += filename
          filelist += (hashValue -> list1.toList)
        }
        else {
          if (!Files.exists(Paths.get(directoryPath + filename + ".tlr"))) createTlr(filename, hashValue, flag = false)
          val list = List(filename)
          filelist += (hashValue -> list)
        }
      }
    }
    listFile = listTempFile.clone
    filelist
  }

}
