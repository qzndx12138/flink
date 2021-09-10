package bean;

/**
 * @author 刘帅
 * @create 2021-09-10 19:10
 */


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {
    private Long orderId;
    private String eventType;
    //交易码
    private String txId;
    private Long eventTime;
}
