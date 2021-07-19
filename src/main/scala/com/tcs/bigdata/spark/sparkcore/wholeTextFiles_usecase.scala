package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object wholeTextFiles_usecase {
  var rareTokens = Set("")
  val stopwords = Set(
    "the","a","an","of","or","in","for","by","on","but", "is", "not", "with", "as", "was", "if",
    "they", "are", "this", "and", "it", "have", "from", "at", "my", "be", "that", "to"
  )
  val regex = """[^0-9]*""".r
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("wholeTextFiles_usecase").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val path = "D:\\bigdata\\datasets\\20news-bydate-train\\*"
    val rdd = sc.wholeTextFiles(path)
    // count the number of records in the dataset
    println(rdd.count)
    /*
    ...
    14/10/12 14:27:54 INFO FileInputFormat: Total input paths to process : 11314
    ...
    11314
    */
    println(rdd.first())
    val newsgroups = rdd.map { case (file, text) => file.split("/").takeRight(2).head }
    println(newsgroups.first())
    val countByGroup = newsgroups.map(n => (n, 1)).reduceByKey(_ + _).collect.sortBy(-_._2).mkString("\n")
    println(countByGroup)
    /*
    (rec.sport.hockey,600)
    (soc.religion.christian,599)
    (rec.motorcycles,598)
    (rec.sport.baseball,597)
    (sci.crypt,595)
    (rec.autos,594)
    (sci.med,594)
    (comp.windows.x,593)
    (sci.space,593)
    (sci.electronics,591)
    (comp.os.ms-windows.misc,591)
    (comp.sys.ibm.pc.hardware,590)
    (misc.forsale,585)
    (comp.graphics,584)
    (comp.sys.mac.hardware,578)
    (talk.politics.mideast,564)
    (talk.politics.guns,546)
    (alt.atheism,480)
    (talk.politics.misc,465)
    (talk.religion.misc,377)
    */

    val text = rdd.map { case (file, text) => text }
    val whiteSpaceSplit = text.flatMap(t => t.split(" ").map(_.toLowerCase))
    println(whiteSpaceSplit.distinct.count)
    // 402978
    // split text on any non-word tokens
    val nonWordSplit = text.flatMap(t => t.split("""\W+""").map(_.toLowerCase))
    println(nonWordSplit.distinct.count)
    // 130126
    // inspect a look at a sample of tokens
    println(nonWordSplit.distinct.sample(true, 0.3, 42).take(100).mkString(","))
    /*
      atheist,resources
      summary:,addresses,,to,atheism
      keywords:,music,,thu,,11:57:19,11:57:19,gmt
      distribution:,cambridge.,290
      archive-name:,atheism/resources
      alt-atheism-archive-name:,december,,,,,,,,,,,,,,,,,,,,,,addresses,addresses,,,,,,,religion,to:,to:,,p.o.,53701.
      telephone:,sell,the,,fish,on,their,cars,,with,and,written
      inside.,3d,plastic,plastic,,evolution,evolution,7119,,,,,san,san,san,mailing,net,who,to,atheist,press
      aap,various,bible,,and,on.,,,one,book,is:
      "the,w.p.,ameri
      can,pp.,,1986.,bible,contains,ball,,based,based,james,of
      */
    /*
bone,k29p,w1w3s1,odwyer,dnj33n,bruns,_congressional,mmejv5,mmejv5,artur,125215,entitlements,beleive,1pqd9hinnbmi,
jxicaijp,b0vp,underscored,believiing,qsins,1472,urtfi,nauseam,tohc4,kielbasa,ao,wargame,seetex,museum,typeset,pgva4,
dcbq,ja_jp,ww4ewa4g,animating,animating,10011100b,10011100b,413,wp3d,wp3d,cannibal,searflame,ets,1qjfnv,6jx,6jx,
detergent,yan,aanp,unaskable,9mf,bowdoin,chov,16mb,createwindow,kjznkh,df,classifieds,hour,cfsmo,santiago,santiago,
1r1d62,almanac_,almanac_,chq,nowadays,formac,formac,bacteriophage,barking,barking,barking,ipmgocj7b,monger,projector,
hama,65e90h8y,homewriter,cl5,1496,zysec,homerific,00ecgillespie,00ecgillespie,mqh0,suspects,steve_mullins,io21087,
funded,liberated,canonical,throng,0hnz,exxon,xtappcontext,mcdcup,mcdcup,5seg,biscuits
*/

    // filter out numbers

    val filterNumbers = nonWordSplit.filter(token => regex.pattern.matcher(token).matches)
    println(filterNumbers.distinct.count)
    // 84912
    println(filterNumbers.distinct.sample(true, 0.3, 42).take(100).mkString(","))
    /*
    reunion,wuair,schwabam,eer,silikian,fuller,sloppiness,crying,crying,beckmans,leymarie,fowl,husky,rlhzrlhz,ignore,
    loyalists,goofed,arius,isgal,dfuller,neurologists,robin,jxicaijp,majorly,nondiscriminatory,akl,sively,adultery,
    urtfi,kielbasa,ao,instantaneous,subscriptions,collins,collins,za_,za_,jmckinney,nonmeasurable,nonmeasurable,
    seetex,kjvar,dcbq,randall_clark,theoreticians,theoreticians,congresswoman,sparcstaton,diccon,nonnemacher,
    arresed,ets,sganet,internship,bombay,keysym,newsserver,connecters,igpp,aichi,impute,impute,raffle,nixdorf,
    nixdorf,amazement,butterfield,geosync,geosync,scoliosis,eng,eng,eng,kjznkh,explorers,antisemites,bombardments,
    abba,caramate,tully,mishandles,wgtn,springer,nkm,nkm,alchoholic,chq,shutdown,bruncati,nowadays,mtearle,eastre,
    discernible,bacteriophage,paradijs,systematically,rluap,rluap,blown,moderates
    */

    // examine potential stopwords
    val tokenCounts = filterNumbers.map(t => (t, 1)).reduceByKey(_ + _)
    val oreringDesc = Ordering.by[(String, Int), Int](_._2)
    println(tokenCounts.top(20)(oreringDesc).mkString("\n"))
    /*
    (the,146532)
    (to,75064)
    (of,69034)
    (a,64195)
    (ax,62406)
    (and,57957)
    (i,53036)
    (in,49402)
    (is,43480)
    (that,39264)
    (it,33638)
    (for,28600)
    (you,26682)
    (from,22670)
    (s,22337)
    (edu,21321)
    (on,20493)
    (this,20121)
    (be,19285)
    (t,18728)
    */
    // filter out stopwords

    val tokenCountsFilteredStopwords = tokenCounts.filter { case (k, v) => !stopwords.contains(k) }
    println(tokenCountsFilteredStopwords.top(20)(oreringDesc).mkString("\n"))
    /*
    (ax,62406)
    (i,53036)
    (you,26682)
    (s,22337)
    (edu,21321)
    (t,18728)
    (m,12756)
    (subject,12264)
    (com,12133)
    (lines,11835)
    (can,11355)
    (organization,11233)
    (re,10534)
    (what,9861)
    (there,9689)
    (x,9332)
    (all,9310)
    (will,9279)
    (we,9227)
    (one,9008)
    */
    // filter out tokens less than 2 characters
    val tokenCountsFilteredSize = tokenCountsFilteredStopwords.filter { case (k, v) => k.size >= 2 }
    println(tokenCountsFilteredSize.top(20)(oreringDesc).mkString("\n"))
    /*
    (ax,62406)
    (you,26682)
    (edu,21321)
    (subject,12264)
    (com,12133)
    (lines,11835)
    (can,11355)
    (organization,11233)
    (re,10534)
    (what,9861)
    (there,9689)
    (all,9310)
    (will,9279)
    (we,9227)
    (one,9008)
    (would,8905)
    (do,8674)
    (he,8441)
    (about,8336)
    (writes,7844)
    */

    // examine tokens with least occurrence
    val oreringAsc = Ordering.by[(String, Int), Int](-_._2)
    println(tokenCountsFilteredSize.top(20)(oreringAsc).mkString("\n"))
    /*
    (lennips,1)
    (bluffing,1)
    (preload,1)
    (altina,1)
    (dan_jacobson,1)
    (vno,1)
    (actu,1)
    (donnalyn,1)
    (ydag,1)
    (mirosoft,1)
    (xiconfiywindow,1)
    (harger,1)
    (feh,1)
    (bankruptcies,1)
    (uncompression,1)
    (d_nibby,1)
    (bunuel,1)
    (odf,1)
    (swith,1)
    (lantastic,1)
    */
    // filter out rare tokens with total occurence < 2
    rareTokens = tokenCounts.filter { case (k, v) => v < 2 }.map { case (k, v) => k }.collect.toSet
    val tokenCountsFilteredAll = tokenCountsFilteredSize.filter { case (k, v) => !rareTokens.contains(k) }
    println(tokenCountsFilteredAll.top(20)(oreringAsc).mkString("\n"))
    /*
    (sina,2)
    (akachhy,2)
    (mvd,2)
    (hizbolah,2)
    (wendel_clark,2)
    (sarkis,2)
    (purposeful,2)
    (feagans,2)
    (wout,2)
    (uneven,2)
    (senna,2)
    (multimeters,2)
    (bushy,2)
    (subdivided,2)
    (coretest,2)
    (oww,2)
    (historicity,2)
    (mmg,2)
    (margitan,2)
    (defiance,2)
    */
    println(tokenCountsFilteredAll.count)
    // 51801
    // create a function to tokenize each document

    // check that our tokenizer achieves the same result as all the steps above
    println(text.flatMap(doc => tokenize(doc)).distinct.count)
    // 51801
    // tokenize each document
    var tokens = text.map(doc => tokenize(doc))

    println(tokens.first.take(20))
    println(text.flatMap(doc => tokenize(doc)).distinct.count)

  }
  def tokenize(line: String): Seq[String] = {
    line.split("""\W+""")
      .map(_.toLowerCase)
      .filter(token => regex.pattern.matcher(token).matches)
      .filterNot(token => stopwords.contains(token))
      .filterNot(token => rareTokens.contains(token))
      .filter(token => token.size >= 2)
      .toSeq

  }
}