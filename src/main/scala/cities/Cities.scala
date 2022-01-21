package cities {

  object CityNames {
    val Madrid = "Madrid"
    val Paris = "Paris"
    val Berlin = "Berlin"
  }

  object CitiesBuilder {
    def fromFn[F](fn: String => F): Cities[F] = {
      Cities(fn(CityNames.Madrid), fn(CityNames.Paris), fn(CityNames.Berlin))
    }
  }

  case class Cities[T](madrid: T, paris: T, berlin: T) {

    def map[F](fn: (T, String) => F) = {
      Cities(fn(madrid, CityNames.Madrid), fn(paris, CityNames.Paris), fn(berlin, CityNames.Berlin))
    }

    def toList() = {
      List(madrid, paris, berlin)
    }
  }
}

