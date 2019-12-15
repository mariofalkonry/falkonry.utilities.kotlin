import java.io.File

fun main(args: Array<String>) {

    // Size threshold
    var maxSize=128*1024*1024

    var filesRoot="C:\\Users\\m2bre\\Documents\\Projects\\JnJ\\2016-2019\\ToSplit"
//    var filesRoot="C:\\Users\\m2bre\\Documents\\Projects\\XOM R&E\\HPPE Share\\Output\\ToShrink"
//    var filesRoot="C:\\Users\\m2bre\\Documents\\Projects\\BP\\Upstream\\Data\\ToShrink"

    // Clear existing output files
    var dir = File("C:\\Users\\m2bre\\Documents\\Projects\\JnJ\\2016-2019\\SmallFiles")
//    var dir = File("C:\\Users\\m2bre\\Documents\\Projects\\XOM R&E\\HPPE Share\\Output\\SmallFiles")
//    var dir = File("C:\\Users\\m2bre\\Documents\\Projects\\BP\\Upstream\\Data\\SmallFiles")
    if(!dir.exists())
        dir.mkdir()
    else {
        dir.listFiles()?.forEach {
            it.delete()
        }
    }

    // Go through files and split to meet maximum size
    File(filesRoot).walk().filter { it.name.endsWith("autoclave.csv") }.forEach {
        var orig=it
        // If size exceeded, split
        if(orig.length()>maxSize)
        {
            // Files to write
            var nfiles=Math.ceil(1.0*orig.length()/maxSize).toInt()

            orig.bufferedReader().use {
                val fin = it
                // Read and and store the header
                val header=fin.readLine()
                var nfile=0
                while(nfile++<nfiles) {
                    // Initialize size tracking
                    var size=header.toByteArray().size
                    // Open file to write to
                    var filePathOut="${dir}\\${orig.name.replace(".","_$nfile.")}"
                    File("$filePathOut").printWriter().use {
                        val fout=it
                        // Write header
                        fout.println(header)
                        // Write lines until lenEach exceeded or no more lines to write (last file)
                        var line:String?=null
                        while ({ line = fin.readLine(); line }() != null) {
                            fout.println(line)//?.replace("null",",")) //
                            // If not last file, check size
                            if(nfile<nfiles) {
                                size += line!!.toCharArray().size
                                if (size >= maxSize)
                                    break
                            }
                        }
                    }
                }
            }
        }
    }
//    var filePath="C:\\Users\\m2bre\\Documents\\Falkonry\\Demos\\Output\\signals_Comp-B.csv"
//    var filePath="C:\\Users\\m2bre\\Documents\\Projects\\BP\\Upstream\\Data\\10272PI.PV_signal.csv"
//    var filePath="C:\\Users\\m2bre\\Documents\\Falkonry\\Demos\\Compressor+Monitoring.csv"
}