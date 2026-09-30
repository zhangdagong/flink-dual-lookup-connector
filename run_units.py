# -*- coding: utf-8 -*-
"""
本项目的离线「编译 + 跑单测」脚本。

环境前提：本机 PATH 里没有 mvn / javac，因此直接用 IDEA 自带的 JBR 里的
javac.exe / java.exe，classpath 取自本地 ~/.m2/repository 下的全部 jar。
不访问网络，不依赖任何外部服务（测试全部用 fake 后端）。

用法：python run_units.py
"""
import glob
import os
import subprocess
import sys

# ---- 硬编码配置（按本机环境） ----
ROOT = r"D:/Data/JavaCode/flink-dual-lookup-connector"
JBR = r"D:/Program/IntelliJ IDEA 2026.2.1/jbr/bin"
M2 = os.path.expanduser("~/.m2/repository").replace("\\", "/")

JAVAC = os.path.join(JBR, "javac.exe")
JAVA = os.path.join(JBR, "java.exe")

MAIN_OUT = os.path.join(ROOT, "target/classes")
TEST_OUT = os.path.join(ROOT, "target/test-classes")
ARGFILE = os.path.join(ROOT, "target/javac_args.txt")

TEST_CLASSES = [
    "com.roc.flink.connector.dual.DualLookupOptionsTest",
    "com.roc.flink.connector.dual.DorisLookupReaderTest",
    "com.roc.flink.connector.dual.DualLookupFunctionTest",
    "com.roc.flink.connector.dual.HBaseRowKeyEncodingTest",
    "com.roc.flink.connector.dual.HBaseLookupReaderTest",
    "com.roc.flink.connector.dual.RowDataConverterTest",
]


def m2_jars():
    """本地仓库里的全部 jar（跳过 sources/javadoc，没必要上 classpath）。"""
    jars = []
    for p in glob.glob(os.path.join(M2, "**/*.jar"), recursive=True):
        p = p.replace("\\", "/")
        if p.endswith("-sources.jar") or p.endswith("-javadoc.jar"):
            continue
        jars.append(p)
    return sorted(jars)


def compile_phase(sources, out_dir, classpath):
    os.makedirs(out_dir, exist_ok=True)
    if not sources:
        return
    # javac 的 @argfile 里反斜杠会被当转义符，必须统一用正斜杠
    with open(ARGFILE, "w", encoding="utf-8") as f:
        f.write("-cp\n%s\n" % classpath)
        f.write("-d\n%s\n" % out_dir.replace("\\", "/"))
        f.write("-encoding\nUTF-8\n")
        for s in sources:
            f.write("%s\n" % s.replace("\\", "/"))
    r = subprocess.run([JAVAC, "@%s" % ARGFILE.replace("\\", "/")],
                       capture_output=True, text=True, errors="replace", cwd=ROOT)
    if r.returncode != 0:
        print(r.stdout)
        print(r.stderr)
        sys.exit("编译失败：%s" % out_dir)


def main():
    jars = m2_jars()
    cp_deps = ";".join([MAIN_OUT.replace("\\", "/")] + jars)

    main_src = glob.glob(os.path.join(ROOT, "src/main/java/**/*.java"), recursive=True)
    test_src = glob.glob(os.path.join(ROOT, "src/test/java/**/*.java"), recursive=True)
    print("[1/3] 编译主代码（%d 个文件）..." % len(main_src))
    compile_phase(main_src, MAIN_OUT, ";".join(jars))

    print("[2/3] 编译测试代码（%d 个文件）..." % len(test_src))
    compile_phase(test_src, TEST_OUT, cp_deps)

    print("[3/3] 运行 %d 个测试类..." % len(TEST_CLASSES))
    cp_run = ";".join([MAIN_OUT.replace("\\", "/"), TEST_OUT.replace("\\", "/")] + jars)
    r = subprocess.run([JAVA, "-cp", cp_run, "org.junit.runner.JUnitCore"] + TEST_CLASSES,
                       capture_output=True, text=True, errors="replace", cwd=ROOT)
    print(r.stdout)
    print(r.stderr)
    sys.exit(r.returncode)


if __name__ == "__main__":
    main()
