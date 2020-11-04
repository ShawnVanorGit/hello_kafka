package deserialization;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/3 18:16
 **/
public class Custom {
    private int customID;
    private String customerName;
    public Custom(int customID, String customerName) {
        super();
        this.customID = customID;
        this.customerName = customerName;
    }
    public int getCustomID() {
        return customID;
    }
    public String getCustomerName() {
        return customerName;
    }

}
