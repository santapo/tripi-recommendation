package recommender.engine.core

import org.apache.commons.lang3.StringUtils.stripAccents
import org.apache.spark.sql.functions.udf

import scala.math._
import scala.util.matching.Regex

class Udf {

}

object Udf {

  val normHotelName = (str: String) => {
    var hotelName = stripAccents(str)
    val patternHotel = new Regex("khach san")
    val patternResort = new Regex("khu nghi duong")
    val patternApartment = new Regex("can ho")
    val patternResidence = new Regex("can ho cao cap")
    val patternCloseBrac = new Regex(".+(?=\\))")


    hotelName = hotelName.toLowerCase()
    hotelName = patternHotel.replaceAllIn(hotelName,"hotel")
    hotelName = patternResort.replaceAllIn(hotelName,"resort")
    hotelName = patternApartment.replaceAllIn(hotelName,"apartment")
    hotelName = patternResidence.replaceAllIn(hotelName,"residence")
    hotelName = hotelName.split(", ").last


    // Doan nay muon lay noi dung trong ngoac
    if (hotelName.contains("(")){
      hotelName = hotelName.split("\\(").last
      hotelName = patternCloseBrac.findAllIn(hotelName).mkString
    }
    hotelName.trim()
  }

  val sigmoidPrice = (avg_price: Float, avg_price_cluster: Float) =>{
    val priceScore = 1/(1+pow(3,-avg_price/avg_price_cluster))
    priceScore
  }

  val sigmoidService = (location_score: Float, service_score: Float, sleep_quality_score: Float, cleanliness_score: Float, meal_score: Float) =>{
    val location_score_score = if(location_score == -1) 0 else location_score
    val service_score_score = if(service_score == -1) 0 else service_score
    val sleep_quality_score_score = if(sleep_quality_score == -1) 0 else sleep_quality_score
    val cleanliness_score_score = if(cleanliness_score == -1) 0 else cleanliness_score
    val meal_score_score = if(meal_score == -1) 0 else meal_score

    val serviceScore = 1/(1+pow(E,-(location_score_score+service_score_score+sleep_quality_score_score+cleanliness_score_score+meal_score_score)/40))

    serviceScore  }

  val mapProvider = (url: String,domain: String,price: String) =>{
    val urlValue = if(url != null) url else ""
    val domainValue = if(domain != null) domain else ""
    val priceValue = if(price != null) price else ""
    val mapPrice = Map("provider"->domainValue,"price"->priceValue,"url"->urlValue)
    mapPrice
  }

  val mapReview = (username: String,domain_id: String,text: String, score: String, review_datetime: String) =>{
    val name = if(username != null) username else ""
    val domain = if(domain_id != null) domain_id else ""
    val review = if(text != null) text else ""
    val overall_score = if(score != null) score else ""
    val date = if(review_datetime != null) review_datetime else ""
    val mapUser = Map("user"->name,"domain"->domain,"review"->review,"overall_score"->overall_score,"date"->date)
    mapUser
  }

  val mapLongitude = (longitude: Float, longitude_mapping: Float) =>{
    val mapLongitude = if((longitude<102) && (longitude>115)) longitude_mapping else longitude
    mapLongitude
  }

  val mapLatitude = (latitude: Float, latitude_mapping: Float) =>{
    val mapLatitude = if((latitude<8) || (latitude>24)) latitude_mapping else latitude
    mapLatitude
  }

  val convertPrice = (price: Float) =>{
    val formatter = java.text.NumberFormat.getIntegerInstance
    val priceString = formatter.format(price).toString
    priceString
  }




  val mapLongitudeUdf = udf(mapLongitude)

  val mapLatitudeUdf = udf(mapLatitude)

  val convertPriceUdf = udf(convertPrice)

  val mapProviderUdf = udf(mapProvider)

  val mapReviewUdf = udf(mapReview)

  val sigmoidServiceUdf = udf(sigmoidService)

  val sigmoidPriceUdf = udf(sigmoidPrice)

  val normHotelNameUdf = udf(normHotelName)
}
