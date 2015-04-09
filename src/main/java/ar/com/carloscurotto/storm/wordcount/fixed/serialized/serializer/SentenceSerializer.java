package ar.com.carloscurotto.storm.wordcount.fixed.serialized.serializer;

import java.util.Collection;

import ar.com.carloscurotto.storm.wordcount.fixed.serialized.domain.Sentence;
import ar.com.carloscurotto.storm.wordcount.fixed.serialized.domain.Word;
import backtype.storm.serialization.types.ArrayListSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;

public class SentenceSerializer extends Serializer<Sentence> {

    private CollectionSerializer collectionSerializer;

    public SentenceSerializer() {
        collectionSerializer = new ArrayListSerializer();
        collectionSerializer.setElementClass(Word.class, new WordSerializer());
    }

    @Override
    public void write(Kryo kryo, Output output, Sentence sentence) {
        collectionSerializer.write(kryo, output, sentence.getWords());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Sentence read(Kryo kryo, Input input, Class<Sentence> clazz) {
        return new Sentence((Collection<Word>)collectionSerializer.read(kryo, input, Collection.class));
    }

}
