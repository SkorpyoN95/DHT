import java.util.Scanner;

public class App {

    public static void main(String args[]) throws Exception{
        System.setProperty("java.net.preferIPv4Stack", "true");
        DistributedMap distro = new DistributedMap();
        boolean run = true;
        Scanner scanner = new Scanner(System.in);
        while(run){
            String command = scanner.nextLine();
            switch(command){
                case "end":
                    distro.finish();
                    run = false;
                    break;
                case "put":
                    String p_key = scanner.nextLine();
                    String p_value = scanner.nextLine();
                    distro.put(p_key,p_value);
                    break;
                case "remove":
                    String r_key = scanner.nextLine();
                    distro.remove(r_key);
                    break;
                case "get":
                    String g_key = scanner.nextLine();
                    System.out.println("Value of " + g_key + " is " + distro.get(g_key));
                    break;
                case "exist":
                    String e_key = scanner.nextLine();
                    System.out.println(e_key + (distro.containsKey(e_key) ? " exists" : " does not exist"));
                    break;
                default:
                    System.out.println("Unrecognised command");
                    break;

            }
        }
    }
}
