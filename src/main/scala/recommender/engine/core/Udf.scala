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

  val getProvinceFirstOrder = (str: String) => {
    var province = stripAccents(str)
    val patternVietNam = new Regex(".+(?=, viet)")
    val patternHanoi = new Regex("hanoi")
    val patternHCM = new Regex("h chi minh")
    val patternD = new Regex("đ|ð")
    val patternSub =  new Regex("(tinh | province|thanh pho | district|khu vuc tp\\. |city| municipality)|s1-1516 |tp.|\\d")
    val patternCloseBrac = new Regex(".+(?=\\))")
    val patternSymbol = new Regex("[^A-Za-z ]")


    province = province.toLowerCase()
    province = patternHanoi.replaceAllIn(province,"ha noi")
    province = patternHCM.replaceAllIn(province,"ho chi minh")
    province = patternSub.replaceAllIn(province, "")
    province = patternD.replaceAllIn(province, "d")

    // Tach viet nam khoi str
    if(province.contains(", viet")){
      province = patternVietNam.findAllIn(province).mkString
    }

    // commune,district,province,country -> province
    province = province.split(", ").last

    if(province.contains("(")){
      if(province.contains("(province)") || province.contains("()")){
        province = province.split("\\(").head
      }else{
        province = province.split(("\\(")).last
        province = patternCloseBrac.findAllIn(province).mkString
      }
    }

    province = patternSymbol.replaceAllIn(province,"")
    // remove whitespace between words and trim string
    province = province.replaceAll("\\s+", " ")

    province.trim()
  }

  val getProvinceSecondOrder = (str: String) => {
    str match {
      case "tan an" => "tien giang"
      case "ba vi" => "ha noi"
      case "quy nhon" => "binh dinh"
      case "da lat" => "lam dong"
      case "dien bien phu" => "dien bien"
      case "hue" | "thua thienhue" => "thue thien hue"
      case "ap thien phuoc" | "mui ne"=> "binh thuan"
      case "ba se" => "can tho"
      case "quan dao cat ba" | "dao cat ba" => "hai phong"
      case "na ngo" => "lai chau"
      case "cam lam" => "khanh hoa"
      case "thon lac nghiep" | "phan rang" => "ninh thuan"
      case "huyen long khanh" => "dong nai"
      case "blao klong ner" => "lam dong"
      case "huyen phu quy" => "binh thuan"
      case "rach gia" | "kien gian" => "kien giang"
      case "sa pa" => "lao cai"
      case "hoi an" | "yen khe" => "ninh binh"
      case "phuc yen" => "vinh phuc"
      case "tan tao" => "ho chi minh"
      case "cua lo" => "nghe an"
      case "hoang hoa" | "sam son" => "thanh hoa"
      case "gia lam pho" => "ha noi"
      case "ngoc quang" => "vinh phuc"
      case "nha trang" => "khanh hoa"
      case "ap phu" => "binh thuan"
      case "bac ha" | "sapa" => "lao cai"
      case "vung tau" | "xa thang tam" | "quan dao con dao" | "ba ria"=> "ba ria vung tau"
      case "moc chau" => "son la"
      case "vinh binh" | "thu dau mot" => "binh duong"
      case "ban na ba" => "nghe an"
      case "dong ha" => "quang tri"
      case "an ma" => "bac kan"
      case "lao san chay" => "yen bai"
      case "ap da thien" => "lam dong"
      case "chau doc" => "an giang"
      case "phu quoc" | "dao phu quoc" | "kien gian" => "kien giang"
      case "tu son" => "bac ninh"
      case "dien chau" => "nghe an"
      case "dong van" | "quan ba" | "meo vac" => "ha giang"
      case "phong nha" => "quang binh"
      case "ha long" => "quang ninh"
      case _ => str
    }
  }

  val sigmoidPrice = (avg_price: Float, avg_price_cluster: Float) =>{
    val priceScore = 1/(1+pow(3,-avg_price/avg_price_cluster))
    priceScore
  }

  val sigmoidService = (relax_spa: Int, relax_massage: Int, relax_outdoor_pool: Int, relax_sauna:Int, cleanliness_score: Float, meal_score: Float) =>{
    val relax_spa_score = if(relax_spa == -1) 0 else relax_spa
    val relax_massage_score = if(relax_massage == -1) 0 else relax_massage
    val relax_outdoor_pool_score = if(relax_outdoor_pool == -1) 0 else relax_outdoor_pool
    val relax_sauna_score = if(relax_sauna == -1) 0 else relax_sauna
    val cleanliness_score_score = if(cleanliness_score == -1) 0 else cleanliness_score
    val meal_score_score = if(meal_score == -1) 0 else meal_score

    val serviceScore = 1/(1+pow(E,-(relax_spa_score+relax_massage_score+relax_outdoor_pool_score+relax_sauna_score+cleanliness_score_score+meal_score_score)/30))

    serviceScore  }

  val mapProvider = (url: String,domain: String,price: String) =>{
    val urlValue = if(url != null) url else ""
    val domainValue = if(domain != null) domain else ""
    val priceValue = if(price != null) price else ""
    val mapPrice = Map("provider"->domainValue,"price"->priceValue,"url"->urlValue)
    mapPrice
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

  val getProvinceFirstOrderUdf = udf(getProvinceFirstOrder)

  val getProvinceSecondOrderUdf = udf(getProvinceSecondOrder)

  val sigmoidServiceUdf = udf(sigmoidService)

  val sigmoidPriceUdf = udf(sigmoidPrice)

  val normHotelNameUdf = udf(normHotelName)
}
