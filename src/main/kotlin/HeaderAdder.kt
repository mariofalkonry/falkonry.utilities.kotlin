import java.io.File

fun main(args: Array<String>) {

    var header="time,020_Autoclave6_Air_Stabilisation_Temp,020_Autoclave6_Atmosferic_Press_ACT,020_Autoclave6_Chamber_Pressure,020_Autoclave6_Chamber_Pressure_Recorder,020_Autoclave6_Chamber_Temperature,020_Autoclave6_Chamber_Temperature_Recorder,020_Autoclave6_Condensor_Temperature,020_Autoclave6_Drain_Temperature,020_Autoclave6_Drain_Temperature_Jacket,020_Autoclave6_Drain_Temperature_Recorder,020_Autoclave6_Drying_Pressure,020_Autoclave6_Drying_Pulses_ACT,020_Autoclave6_Filter_Pressure,020_Autoclave6_Filter_Pulses_ACT,020_Autoclave6_Filter_Temperature,020_Autoclave6_Jacket_Temperature,020_Autoclave6_Leak_Rate_ACT,020_Autoclave6_PreVacuum_Pulse_ACT,020_Autoclave6_Sterile_Time_ACT,020_Autoclave6_Vacuum_Level_ACT,020_Autoclave7_Air_Stabilisation_Temp,020_Autoclave7_Atmosferic_Press_ACT,020_Autoclave7_Chamber_Pressure,020_Autoclave7_Chamber_Pressure_Recorder,020_Autoclave7_Chamber_Temperature,020_Autoclave7_Chamber_Temperature_Recorder,020_Autoclave7_Condensor_Temperature,020_Autoclave7_Drain_Temperature,020_Autoclave7_Drain_Temperature_Jacket,020_Autoclave7_Drain_Temperature_Recorder,020_Autoclave7_Drying_Pressure,020_Autoclave7_Drying_Pulses_ACT,020_Autoclave7_Filter_Pressure,020_Autoclave7_Filter_Pulses_ACT,020_Autoclave7_Filter_Temperature,020_Autoclave7_Jacket_Temperature,020_Autoclave7_Leak_Rate_ACT,020_Autoclave7_PreVacuum_Pulse_ACT,020_Autoclave7_Sterile_Time_ACT,020_Autoclave7_Vacuum_Level_ACT,020_Autoclave8_Air_Stabilisation_Temp,020_Autoclave8_Atmosferic_Press_ACT,020_Autoclave8_Chamber_Pressure,020_Autoclave8_Chamber_Pressure_Recorder,020_Autoclave8_Chamber_Temperature,020_Autoclave8_Chamber_Temperature_Recorder,020_Autoclave8_Condensor_Temperature,020_Autoclave8_Drain_Temperature,020_Autoclave8_Drain_Temperature_Jacket,020_Autoclave8_Drain_Temperature_Recorder,020_Autoclave8_Drying_Pressure,020_Autoclave8_Drying_Pulses_ACT,020_Autoclave8_Filter_Pressure,020_Autoclave8_Filter_Pulses_ACT,020_Autoclave8_Filter_Temperature,020_Autoclave8_Jacket_Temperature,020_Autoclave8_Leak_Rate_ACT,020_Autoclave8_PreVacuum_Pulse_ACT,020_Autoclave8_Sterile_Time_ACT,020_Autoclave8_Vacuum_Level_ACT"


    var filesRoot="C:\\Users\\m2bre\\Documents\\Projects\\JnJ\\2016-2019"
//    var filesRoot="C:\\Users\\m2bre\\Documents\\Projects\\Molex\\Molding\\ToSplit"
//    var filesRoot="C:\\Users\\m2bre\\Documents\\Projects\\XOM R&E\\HPPE Share\\Output\\ToShrink"
//    var filesRoot="C:\\Users\\m2bre\\Documents\\Projects\\BP\\Upstream\\Data\\ToShrink"

    // Clear existing output files
    var dir = File("C:\\Users\\m2bre\\Documents\\Projects\\JnJ\\2016-2019")
//    var dir = File("C:\\Users\\m2bre\\Documents\\Projects\\XOM R&E\\HPPE Share\\Output\\SmallFiles")
//    var dir = File("C:\\Users\\m2bre\\Documents\\Projects\\BP\\Upstream\\Data\\SmallFiles")
    if(!dir.exists())
        dir.mkdir()
    else {
        dir.listFiles()?.forEach {
            if(it.name.endsWith("*_header.csv"))
                it.delete()
        }
    }

    // Go through files and add header if missing
    File(filesRoot).walk().filter { it.name.endsWith(".csv") && !it.name.contains("_header")}.forEach {
        var orig=it
        var firstLine=true
        // Open file to write to
        var filePathOut="${dir}\\${orig.name.replace(".csv","_header.csv")}"
        File("$filePathOut").printWriter().use {
            var fout=it
            orig.forEachLine {
                var line = it
                if (firstLine) {
                    if (line != header) {
                        fout.println(header)
                        fout.println(line)
                    }
                    else
                        fout.println(line)
                    firstLine=false
                }
                else
                    fout.println(line)
            }
        }
    }
}