package pojo;

import java.sql.Timestamp;

public class Behavior {
    public Long user_id;
    public Long item_id;
    public Long category_id;
    public String behavior;
    public Timestamp ts;
    public Timestamp proctime;


    @Override
    public String toString() {
        return "Behavior{" +
                "user_id=" + user_id +
                ", item_id=" + item_id +
                ", category_id=" + category_id +
                ", behavior='" + behavior + '\'' +
                ", ts=" + ts +
                ", proctime=" + proctime +
                '}';
    }
}
