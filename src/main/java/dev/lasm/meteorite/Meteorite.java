package dev.lasm.meteorite;

import net.minecraft.init.Blocks;
import net.minecraft.world.gen.ChunkProviderServer;
import net.minecraftforge.event.world.WorldEvent;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.common.Mod.EventHandler;
import net.minecraftforge.fml.common.event.FMLInitializationEvent;
import net.minecraftforge.fml.common.event.FMLPreInitializationEvent;
import net.minecraftforge.fml.common.eventhandler.SubscribeEvent;
import org.apache.logging.log4j.Logger;
import org.rocksdb.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Mod(modid = Meteorite.MODID, name = Meteorite.NAME, version = Meteorite.VERSION)
@Mod.EventBusSubscriber
public class Meteorite {
    public static final String MODID = "meteorite";
    public static final String NAME = "Meteorite";
    public static final String VERSION = "1.0";

    @Mod.Instance
    public static Meteorite INSTANCE;

    static {
        RocksDB.loadLibrary();
    }

    public Meteorite() {
        INSTANCE = this;
    }

    public static Logger logger;

    public static final Options options = new Options().setWalRecoveryMode(WALRecoveryMode.PointInTimeRecovery).setCompressionType(CompressionType.ZSTD_COMPRESSION).setCreateIfMissing(true);

    @EventHandler
    public void preInit(FMLPreInitializationEvent event) {
        logger = event.getModLog();
    }

    @EventHandler
    public void init(FMLInitializationEvent event) {
        // some example code
        logger.info("DIRT BLOCK >> {}", Blocks.DIRT.getRegistryName());
    }

    public static Map<String, RocksDB> dbHashMap = new ConcurrentHashMap<>();

    public static RocksDB getOrCreateDb(String key) {
        if (dbHashMap.containsKey(key))
            return dbHashMap.get(key);

        try {
            var db = RocksDB.open(Meteorite.options, key);
            dbHashMap.put(key, db);
            return db;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeDb(String path) {
        var db = dbHashMap.get(path);
        if (db != null) {
            db.close();

            dbHashMap.remove(path);
        }
    }

    @SubscribeEvent
    public static void loadWorld(WorldEvent.Load event) {

    }

    @SubscribeEvent
    public static void unloadWorld(WorldEvent.Unload event) {


        if (event.getWorld().getChunkProvider() instanceof ChunkProviderServer provider) {
            if (provider.chunkLoader instanceof DatabaseProvider loader) {
                try {
                    // FIXME closeDb(loader.getDatabaseId());

                    dbHashMap.remove(loader.getDatabaseId());
                    loader.queueClose();

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
