package ru.fediq.example.scalding1

import java.net.URL

import com.twitter.scalding.typed._
import com.twitter.scalding.{Args, Job, TypedTsv}
import scala.collection.mutable.ArrayBuffer

object TopicSwitchesJob {
  type Domain = String
  type UserId = String
  type Topic = String
  type Url = String
  type Timestamp = Long

  case class Click(url: Url, ts: Timestamp, userId: UserId)

  case class SiteInfo(domain: Domain, topic: Topic)

  case class TopicActivity(topic: Topic, ts: Timestamp, userId: UserId)

  case class Helper(topic: Option[Topic] = None, firstTs: Timestamp = 0l, firstVisit: Boolean = false)

  def url2domain(url: Url) = new URL(url).getHost
}

class TopicSwitchesJob(args: Args) extends Job(args) {
  import TopicSwitchesJob._

  val pathToClicks = args.required("clicks")
  val pathToSites = args.required("sites")
  val outputPath = args.required("output")

  val clicksPipe: TypedPipe[Click] =
    TypedPipe.from(TypedTsv[(Url, Timestamp, UserId)](pathToClicks))
      .map(tuple => Click.tupled(tuple))

  val domainsPipe: TypedPipe[Click] =
    clicksPipe
      .map(click => click.copy(url = url2domain(click.url)))

  val sitesGroupByDomain: Grouped[Domain, SiteInfo] =
    TypedPipe.from(TypedTsv[(Domain, Topic)](pathToSites))
      .map(tuple => SiteInfo.tupled(tuple))
      .groupBy(siteInfo => siteInfo.domain)

  val clicksWithSiteInfo: TypedPipe[(Domain, (Click, SiteInfo))] =
    domainsPipe
      .map(click => (click.url, click))
      .hashJoin(sitesGroupByDomain)

  val topicActivityStreamPerUser: SortedGrouped[UserId, TopicActivity] =
    clicksWithSiteInfo
      .map(tuple => {
        val (domain, (click, siteInfo)) = tuple
        TopicActivity(siteInfo.topic, click.ts, click.userId)
      })
      .groupBy(activity => activity.userId)
      .sortBy(activity => activity.ts)

  def topicSwitches(userId: UserId, activities: Iterator[TopicActivity]): Iterator[TopicActivity] = {
    val result = ArrayBuffer[TopicActivity]()
    var firstTs = 0l
    var lastTopic = null.asInstanceOf[Topic]
    for (activity <- activities) {
      if (firstTs == 0l || lastTopic != activity.topic) {
        result.append(activity)
        firstTs = activity.ts
        lastTopic = activity.topic
      }
    }
    result.toIterator
  }

  val firstTopicActivitiesPipe: TypedPipe[TopicActivity] =
    topicActivityStreamPerUser
      .mapGroup((userId, activities) => topicSwitches(userId, activities))
      .values

  firstTopicActivitiesPipe
    .map(activity => (activity.topic, activity.ts, activity.userId))
    .write(TypedTsv(outputPath))
}

class ScalaWayTopicSwitchesJob(args: Args) extends Job(args) {
  import TopicSwitchesJob._

  val pathToClicks = args.required("clicks")
  val pathToSites = args.required("sites")
  val outputPath = args.required("output")

  def topicSwitches(userId: UserId, activities: Iterator[TopicActivity]): Iterator[TopicActivity] = {
    activities.scanLeft(Helper())((helper, activity) => {
      if (helper.topic.isEmpty || helper.topic.get != activity.topic) {
        Helper(Some(activity.topic), activity.ts, true)
      } else {
        Helper(helper.topic, helper.firstTs, false)
      }
    }).filter(_.firstVisit).map(helper => TopicActivity(helper.topic.get, helper.firstTs, userId))
  }

  TypedPipe.from(TypedTsv[(Url, Timestamp, UserId)](pathToClicks))
    .map(tuple => Click.tupled(tuple))
    .map(click => click.copy(url = new URL(click.url).getHost))
    .map(click => (click.url, click))
    .hashJoin(
      TypedPipe.from(TypedTsv[(Domain, Topic)](pathToSites))
        .map(tuple => SiteInfo.tupled(tuple))
        .groupBy(_.domain)
    )
    .map({case (_, (click, siteInfo)) => TopicActivity(siteInfo.topic, click.ts, click.userId)})
    .groupBy(_.userId)
    .sortBy(_.ts)
    .mapGroup(topicSwitches)
    .values
    .write(TypedTsv(outputPath))
}
