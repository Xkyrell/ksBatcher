package me.xkyrell.batcher;

import org.jetbrains.annotations.ApiStatus;
import java.util.NoSuchElementException;

@ApiStatus.NonExtendable
public class KeyNotLoadedException extends NoSuchElementException {

    public KeyNotLoadedException(Object key) {
        super("No value was returned for key: " + key);
    }
}
