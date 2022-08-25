package om.zg.exercise;

import lombok.*;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.exercise
 * @date 2022-08-22-21:49
 * @Desc:
 */
public class KeyMaoEx {
    public static void main(String[] args) {

        EventBean a = new EventBean(1L, "a", 10L, 100);
        EventBean b = new EventBean(1L, "a", 10L, 101);
        EventBean c = new EventBean(1L, "a", 10L, 102);
        EventBean d = new EventBean(1L, "a", 10L, 104);
        TreeMap<Integer, EventBean> longEventBeanTreeMap = new TreeMap<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 > o2 ? -1 : (o1 < o2) ? 1 : 0;
            }
        });
        System.out.println(longEventBeanTreeMap.size());

        for (int i = 0; i < 3; i++) {
            longEventBeanTreeMap.put(a.getActTime(), a);
            Iterator<Integer> iterator = longEventBeanTreeMap.keySet().iterator();
            Integer next = iterator.next();
            System.out.println(longEventBeanTreeMap.get(next));

        }

        longEventBeanTreeMap.put(b.getActTime(), b);
        longEventBeanTreeMap.put(a.getActTime(), a);
        longEventBeanTreeMap.put(c.getActTime(), c);
        longEventBeanTreeMap.put(d.getActTime(), d);
//
//        longEventBeanTreeMap.forEach((key, value) -> System.out.print(key + "=" + value + "\t"));
//        System.out.println(longEventBeanTreeMap.size());
//        Iterator<Integer> iterator = longEventBeanTreeMap.keySet().iterator();
//
//        for (int i = 0; i < longEventBeanTreeMap.size(); i++) {
//            Integer next = iterator.next();
//            System.out.println(longEventBeanTreeMap.get(next));
//        }

        System.out.println("-----------");

        for (int i = 0; i < longEventBeanTreeMap.size(); i++) {
        Iterator<Integer> iterator2 = longEventBeanTreeMap.keySet().iterator();
            Integer next = iterator2.next();
            System.out.println(next+"iter");
            System.out.println(longEventBeanTreeMap.get(next));

        }


        Iterator<Integer> iterator3 = longEventBeanTreeMap.keySet().iterator();
        System.out.println("###############");
        Integer next11 = iterator3.next();
        System.out.println(longEventBeanTreeMap.get(next11));


    }
}


@Data
@Getter
@Setter
@AllArgsConstructor
@ToString
class EventBean {
    private Long guid;
    private String eventId;
    private Long timestamp;
    private Integer ActTime;
}