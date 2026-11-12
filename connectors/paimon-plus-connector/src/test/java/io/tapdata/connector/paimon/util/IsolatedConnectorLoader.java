package io.tapdata.connector.paimon.util;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * 模拟 PDK 为同一 Connector jar 再建一个 ClassLoader：父加载器为 platform loader，Connector 与
 * Paimon 类由本加载器重新定义，因此 static 状态（如 LIVE_DIRS）与应用加载器互不共享；
 * JDK 自身的类（包括 FileChannel 的 JVM 级 FileLockTable）仍由 bootstrap/platform 共享。
 */
final class IsolatedConnectorLoader {

    private IsolatedConnectorLoader() {
    }

    static URLClassLoader create() throws MalformedURLException {
        List<URL> urls = new ArrayList<>();
        for (String entry : SpillProbeProcess.testClassPath().split(File.pathSeparator)) {
            if (!entry.trim().isEmpty()) {
                urls.add(new File(entry).toURI().toURL());
            }
        }
        return new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader());
    }
}
