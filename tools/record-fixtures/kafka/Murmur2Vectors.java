// Prints, for each hex-encoded key on stdin, the Java client's own murmur2 and partition choice
// for each partition count in args: `<hex> <murmur2 i32> <partition per n, comma-separated>`.
// `script/record-fixtures kafka` compiles it against the broker image's kafka-clients jar and
// runs it there, so the vectors come from the jar a Java producer of that release uses.
import java.io.*;
import java.util.HexFormat;
import org.apache.kafka.common.utils.Utils;

public class Murmur2Vectors {
    public static void main(String[] args) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = in.readLine()) != null) {
            byte[] key = HexFormat.of().parseHex(line.trim());
            int h = Utils.murmur2(key);
            StringBuilder sb = new StringBuilder();
            for (String n : args) {
                if (sb.length() > 0) sb.append(',');
                sb.append(Utils.toPositive(h) % Integer.parseInt(n));
            }
            System.out.println(line.trim() + " " + h + " " + sb);
        }
    }
}
