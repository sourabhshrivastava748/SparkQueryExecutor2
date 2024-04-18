object StringUtils {

    val removePlus91 = (inputString: String) => {
        inputString.substring(3)
    }

    val addQuotes = (inputString: String) => {
        "'" + inputString + "'"
    }

}
