package application.traffic_analysis;

public class TrafficFlow {
    public Integer id;
    public String device_id;
    public String car_no;
    public String take_photo_at;
    public Integer num = 1;

    public TrafficFlow() {
    }

    public TrafficFlow(Integer id, String device_id, String car_no, String take_photo_at) {
        this.id = id;
        this.device_id = device_id;
        this.car_no = car_no;
        this.take_photo_at = take_photo_at;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public String getCar_no() {
        return car_no;
    }

    public void setCar_no(String car_no) {
        this.car_no = car_no;
    }

    public String getTake_photo_at() {
        return take_photo_at;
    }

    public void setTake_photo_at(String take_photo_at) {
        this.take_photo_at = take_photo_at;
    }

    @Override
    public String toString() {
        return "TrafficFlow{" +
                "id=" + id +
                ", device_id='" + device_id + '\'' +
                ", car_no='" + car_no + '\'' +
                ", take_photo_at='" + take_photo_at + '\'' +
                ", num=" + num +
                '}';
    }
}
