package Brazil;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Manager {
    private static Map<String, String> RESOURCESS;
    private static Scanner iuput = new Scanner(System.in);

    static {
        RESOURCESS = new HashMap<String, String>();
        RESOURCESS.put("Step1_input", "/Brazil_weather");
        RESOURCESS.put("Step1_output", "/Step1_output");
        RESOURCESS.put("Step2_input", "/Step1_output");
        RESOURCESS.put("Step2_output", "/Step2_output");
        RESOURCESS.put("Step3_input", "/Step1_output");
        RESOURCESS.put("Step3_output", "/Step3_output");
        RESOURCESS.put("Step4_input", "/Step1_output");
        RESOURCESS.put("Step4_output", "/Step4_output");
    }

    public static void showMenu() {
        boolean flag = false;
        int choice = 0;
        do {
            System.out.println("O(∩_∩)O天气综合管理系统O(∩_∩)O");
            System.out.println("1、数据清洗与导入");
            System.out.println("2、查询指定历史日期的天气数据");
            System.out.println("3、求每年的最高温度、每年的最低温度、每年的平均温度、每年的降雨天数");
            System.out.println("4、预测指定日期天气");
            System.out.println("0、退出系统");
            System.out.println("请选择（0--4）：");
            choice = iuput.nextInt();
            switch (choice) {
                case 0:
                    flag=!flag;
                    System.out.println("感谢您的使用，谢谢");
                    break;
                case 1:
                    //运行Step1:数据清洗与导入
                    Step1.run(RESOURCESS.get("Step1_input"), RESOURCESS.get("Step1_output"));
                    break;
                case 2:
                    //运行Step2：查询指定历史日期的天气数据
                    Step2.run(RESOURCESS.get("Step2_input"), RESOURCESS.get("Step2_output"));
                    break;
                case 3:
                    //运行Step3：求每年的最高温度、每年的最低温度、每年的平均温度、每年的降雨天数
                    Step3.run(RESOURCESS.get("Step3_input"), RESOURCESS.get("Step3_output"));
                    break;
                case 4:
                    //运行Step4：预测指定日期天气
                    Step4.run(RESOURCESS.get("Step4_input"), RESOURCESS.get("Step4_output"));
                    break;
                default:
                    System.out.println("输入错误＞﹏＜");
                    break;
            }
        } while (!flag);
    }

    public static void main(String[] args) {
        showMenu();
    }
}
