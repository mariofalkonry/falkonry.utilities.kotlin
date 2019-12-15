import java.io.File

fun main(args: Array<String>) {
    // Files from PI to read - the tag name is in the file name
    // Examples: NEP_SOL_AN_Comb_Bearing_1_Ax_Disp_Values.csv, NEP_SOL_AP_HPC_Flow_Act_Values.csv, etc.
    var fileDir="C:\\Users\\m2bre\\Documents\\Projects\\BHP\\Data"

    /* Lambda Functions */
    // The tag name is the name of the file minus .CSV
    var tagNameGet:(File)->String={f->f.name.substring(f.name.indexOf("SOL_A")+7,f.name.indexOf("_Values.csv"))}
    // The output files that will be created are the same name a exist plus _signal
    var fileNameGet:(File)->String={f->"${f.path.substring(0,f.path.indexOf(".csv"))}_signal.csv"}


    // Header to put in every file
    var header="timestamp,value,signal"

    // Cleanup from last run
    File(fileDir).walk().forEach {
        if (it.isFile && it.name.endsWith("_signal.csv"))
            it.delete()
    }


    File(fileDir).walk().filter { it.name.endsWith("Values.csv") && !it.name.endsWith("_signal.csv")}.forEach {
        var fin=it
        fin.bufferedReader().use {
            // Open output file stream
            var reader=it
            var filePathOut=fileNameGet(fin)
            if(File("$filePathOut").exists())
                File("$filePathOut").delete()
            // Get tag name
            var tag=tagNameGet(fin)
            File("$filePathOut").printWriter().use {
                // Write header
                val fout=it
                // Write header
                fout.println(header)
                // Write lines until lenEach exceeded or no more lines to write (last file)
                var line:String?=null
                var firstLine=true
                while ({ line = reader.readLine(); line }() != null) {
                    if(!firstLine) {
                        var parts=line?.split(",")
                        var value=parts?.getOrNull(1)?.trim('"')?.toDoubleOrNull()
                        if(value!=null)
                            fout.println("${parts?.get(0)},$value,$tag")
                    }
                    else
                        firstLine=false
                }
            }
        }
    }
}