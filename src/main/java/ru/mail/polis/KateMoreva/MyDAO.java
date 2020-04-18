package ru.mail.polis.KateMoreva;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
public class MyDAO implements DAO {
    private final SortedMap<ByteBuffer, ByteBuffer> map = new ConcurrentSkipListMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from){
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(element -> Record.of(Objects.requireNonNull(element).getKey(), element.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value){
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key){
        map.remove(key);
    }

    @Override
    public void close(){
        map.clear();
    }
}
