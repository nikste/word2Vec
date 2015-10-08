package w2v;

import java.util.Arrays;

class TwitterTrainInstance{
    public String[] HashTags;
    public String[] Text;

    public TwitterTrainInstance(String[] hashTags, String[] text) {
        HashTags = hashTags;
        Text = text;
    }


    @Override
    public String toString() {
        return "TwitterTrainInstance{" +
                "HashTags=" + Arrays.toString(HashTags) +
                ", Text=" + Arrays.toString(Text) +
                '}';
    }
}