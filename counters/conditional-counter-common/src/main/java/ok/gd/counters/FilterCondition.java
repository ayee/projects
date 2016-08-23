package ok.gd.counters;

/**
 * Created by olegklymchuk on 8/22/16.
 *
 */

public class FilterCondition {

    int[] allIndices;
    int[] anyIndices;
    int[] noneIndices;

    public FilterCondition() {}

    public FilterCondition(int[] allIndices, int[] anyIndices, int[] noneIndices) {

        this.allIndices = allIndices;
        this.anyIndices = anyIndices;
        this.noneIndices = noneIndices;
    }

    public boolean check(Record record) {

        if(allIndices != null) {
            for(int i : allIndices) {
                if(!record.checkAttribute(i)) {
                    return false;
                }
            }
        }

        if(noneIndices != null) {
            for(int i : noneIndices) {
                if(record.checkAttribute(i)) {
                    return false;
                }
            }
        }

        if(anyIndices != null) {
            for(int i : anyIndices) {
                if(record.checkAttribute(i)) {
                    return true;
                }
            }

            return false;
        }

        return true;
    }
}
