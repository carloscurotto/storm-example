package ar.com.carloscurotto.storm.wordcount.serialized.serializer;

import ar.com.carloscurotto.storm.wordcount.serialized.domain.Word;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class WordSerializer extends Serializer<Word> {

    @Override
    public void write(Kryo kryo, Output output, Word word) {
        output.writeString(word.getValue());
    }

    @Override
    public Word read(Kryo kryo, Input input, Class<Word> clazz) {
        return new Word(input.readString());
    }

}
