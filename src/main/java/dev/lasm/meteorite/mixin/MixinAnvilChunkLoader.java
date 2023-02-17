package dev.lasm.meteorite.mixin;

import com.google.common.io.ByteStreams;
import dev.lasm.meteorite.DatabaseProvider;
import dev.lasm.meteorite.Meteorite;
import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTSizeTracker;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.util.datafix.DataFixer;
import net.minecraft.util.datafix.FixTypes;
import net.minecraft.util.math.ChunkPos;
import net.minecraft.world.World;
import net.minecraft.world.chunk.storage.AnvilChunkLoader;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

@Mixin(AnvilChunkLoader.class)
public abstract class MixinAnvilChunkLoader implements DatabaseProvider {
    private RocksDB db;

    private String path;

    @Inject(method = "<init>", at = @At("RETURN"))
    public void onInit(File chunkSaveLocationIn, DataFixer dataFixerIn, CallbackInfo ci) {
        path = Paths.get(chunkSaveLocationIn.getAbsolutePath(), "rocksdb").toString();
        db = Meteorite.getOrCreateDb(path);
    }

    @Inject(method = "writeChunkData", at = @At("HEAD"), cancellable = true)
    public void onWriteChunkData(ChunkPos pos, NBTTagCompound compound, CallbackInfo ci) {

        var output = ByteStreams.newDataOutput();
        try {
            CompressedStreamTools.write(compound, output);

            db.put(toByteArray(pos.x, pos.z), output.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ci.cancel();
    }

    @Final
    @Shadow
    private Map<ChunkPos, NBTTagCompound> chunksToSave;

    @Final
    @Shadow
    private DataFixer fixer;


    @Inject(method = "loadChunk__Async", at = @At("HEAD"), cancellable = true)
    public void onLoadChunk(World worldIn, int x, int z, CallbackInfoReturnable<Object[]> ci) throws IOException {
        ChunkPos chunkpos = new ChunkPos(x, z);
        NBTTagCompound nbttagcompound = chunksToSave.get(chunkpos);

        if (nbttagcompound == null)
        {
            byte[] data;
            try {
                data = db.get(toByteArray(x, z));

                if (data == null) {
                    ci.setReturnValue(null);
                    ci.cancel();
                    return;
                }

                var compound = CompressedStreamTools.read(ByteStreams.newDataInput(data), NBTSizeTracker.INFINITE);

                nbttagcompound = fixer.process(FixTypes.CHUNK, compound);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        ci.setReturnValue(this.checkedReadChunkFromNBT__Async(worldIn, x, z, nbttagcompound));
        ci.cancel();
    }

    @Shadow
    protected abstract Object[] checkedReadChunkFromNBT__Async(World worldIn, int x, int z, NBTTagCompound nbttagcompound);

    @Inject(method = "flush", at = @At("RETURN"))
    public void onFlush(CallbackInfo ci) {
        try (var options = new FlushOptions().setWaitForFlush(true)) {
            db.flush(options);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] toByteArray(int x, int z) {
        return new byte[] {
            (byte) (x >> 24), (byte) (x >> 16), (byte) (x >> 8), (byte) x,
            (byte) (z >> 24), (byte) (z >> 16), (byte) (z >> 8), (byte) z,
        };
    }

    @Override
    public RocksDB getDb() {
        return this.db;
    }

    @Override
    public String getDatabaseId() {
        return path;
    }

    private boolean close = false;

    @Override
    public void queueClose() {
        close = true;
    }

    @Override
    protected void finalize() throws Throwable {
        if (close) {
            db.close();
            db = null;
        }
    }
}
