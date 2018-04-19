package slicer

import java.util.Timer
import java.util.TimerTask

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable.Set
import scala.concurrent.Await
import javafx.embed.swing.JFXPanel
import javafx.scene.{chart => jfxsc}

import scalafx.Includes._
import scalafx.application.Platform
import scalafx.beans.property.{LongProperty, StringProperty}
import scalafx.beans.value.ObservableValue
import scalafx.collections.ObservableBuffer
import scalafx.collections.ObservableMap
import scalafx.geometry.Insets
import scalafx.scene.Scene
import scalafx.scene.control.{TableColumn, TableView}
import scalafx.scene.control.TableColumn._
import scalafx.scene.chart.{BarChart, CategoryAxis, LineChart, NumberAxis, XYChart}
import scalafx.scene.layout.BorderPane
import scalafx.stage.Stage
import scalafx.util.StringConverter

class PartitionProperty(serverName_ : String,
                        firstIndex_ : Long,
                        size_ : Long) {
  val serverName = new StringProperty(this, "serverName", serverName_)
  val firstIndex = new LongProperty(this, "firstIndex", firstIndex_)
  val lastIndex = new LongProperty(this, "lastIndex", firstIndex_ + size_)
}

object PartitionProperty {
  implicit def ordering[A <: PartitionProperty]: Ordering[A] = Ordering.by(p => (p.serverName.value, p.firstIndex.value))
}

class DataGUI(var shardingService: ActorRef)(implicit val timeout: Timeout) {
  Platform.implicitExit = false
  // Shortcut to initialize JavaFX, force initialization by creating JFXPanel() object
  // (we will not use it for anything else)
  new JFXPanel()
  var timer : Timer = null

  def updateService(service: ActorRef) = { shardingService = service }

  def start = {
    Platform.runLater {
      val utilizationNames = ObservableBuffer[String]()
      val utilizationData = ObservableMap[String, Int]()
      val partitionData = ObservableBuffer[PartitionProperty]()

      val observableSeries = ObservableBuffer[jfxsc.XYChart.Series[String, Number]](new XYChart.Series[String, Number]())
      val observableChurnSeries = ObservableBuffer[jfxsc.XYChart.Series[Number, Number]](new XYChart.Series[Number, Number]())

      timer = new Timer()
      timer.scheduleAtFixedRate(new TimerTask () {
        def run() = {
          Platform.runLater {
            val datafut = shardingService ? RequestUtilizations
            val response = Await.result(datafut, timeout.duration).asInstanceOf[(Map[String, (Double, Seq[(Long, Long)])], Double)]

            val data = response._1
            val churn = response._2

            if (!data.isEmpty) {
              val toClearNames = Set(utilizationNames.toList: _*)
              partitionData.clear

              data.toSeq.sortBy(_._1).foreach(entry => {
                val key = entry._1

                if (!toClearNames.contains(key)) {
                  utilizationNames += key
                } else {
                  toClearNames -= key
                }

                val sortedValues = entry._2._2.sortBy(p => (p._1, p._2))
                sortedValues.foreach(v => partitionData += new PartitionProperty(key, v._1, v._2))

                val newValue = entry._2._1 * 100
                val newData = XYChart.Data[String, Number](key, newValue)
                if (utilizationData.contains(key)) {
                  val index = utilizationData(key)
                  val currValue = observableSeries(0).data()(index).getYValue
                  if (Math.abs(currValue.doubleValue - newValue) > 0.1) {
                    observableSeries(0).data().update(index, newData);
                  }
                } else {
                  val newIndex = utilizationData.size
                  utilizationData.put(key, newIndex)
                  observableSeries(0).data() += newData;
                }
              })

              utilizationNames --= toClearNames
              utilizationNames.sorted
            }

            observableChurnSeries(0).data() += XYChart.Data[Number, Number](observableChurnSeries(0).data().size, (churn * 100))
          }
        }
      }, 0, 2000)

      val xAxis = new CategoryAxis {
        label = "Servers"
        categories = utilizationNames
      }
      val yAxis = new NumberAxis {
        label = "Utilization"
        autoRanging = false
        lowerBound = 0
        upperBound = 100
      }

      val xNumbAxis = new NumberAxis {
        label = "Updates"
        tickUnit = 1
        tickLength = 1
        minorTickVisible = false
        tickLabelFormatter = (new StringConverter[Number] {
          def toString(v: Number) : String = {
            if (v.doubleValue.isWhole) {
              "" + v.intValue
            } else {
              ""
            }
          }

          def fromString(str: String) : Number = {
            str.toDouble.toInt
          }
        })
      }
      val yNumbAxis = new NumberAxis {
        label = "Churn"
        lowerBound = 0
        upperBound = 100
      }

      val barChart = new BarChart[String, Number](xAxis, yAxis, observableSeries) {
        barGap = 5
        title = "Utilization Per Server"
        animated = false
        legendVisible = false
      }

      val tableView = new TableView[PartitionProperty](partitionData) {
        columns ++= List(
          new TableColumn[PartitionProperty, String] {
            text = "Server Name"
            cellValueFactory = _.value.serverName
            prefWidth = 100
          },
          new TableColumn[PartitionProperty, Long] {
            text = "First Index"
            cellValueFactory = _.value.firstIndex.asInstanceOf[ObservableValue[Long, Long]]
            prefWidth = 100
          },
          new TableColumn[PartitionProperty, Long] {
            text = "Last Index"
            cellValueFactory = _.value.lastIndex.asInstanceOf[ObservableValue[Long, Long]]
            prefWidth = 100
          }
        )
      }

      val lineChart = new LineChart(xNumbAxis, yNumbAxis, observableChurnSeries) {
        title = "Churn per update"
        legendVisible = false
      }

      val guiStage = new Stage {
        outer =>
        title = "Data Visualization"
        scene = new Scene(1000, 900) {
          root = new BorderPane {
            padding = Insets(25)
            left = barChart
            right = tableView
            bottom = lineChart
          }
        }
      }
      guiStage.centerOnScreen()
      guiStage.y = 10

      guiStage.show()
    }
  }

  def stop = {
    Platform.exit()
    if (timer != null) timer.cancel
  }
}
