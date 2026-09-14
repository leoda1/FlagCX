import os
import shutil
import sys

# Disable auto load flagcx when setup
os.environ["TORCH_DEVICE_BACKEND_AUTOLOAD"] = "0"

# Modern setuptools (>=64) uses pip for 'develop' which creates isolated build envs.
# For packages depending on torch, this often fails.
# We try to disable build isolation if not explicitly set.
if "PIP_NO_BUILD_ISOLATION" not in os.environ:
    os.environ["PIP_NO_BUILD_ISOLATION"] = "1"

from setuptools import setup, find_packages
from _build_config import (
    ADAPTOR_MAP,
    detect_adaptor,
    detect_torch_flag,
    get_device_config,
    get_device_rpath_dirs,
    get_ext_classes,
    resolve_torch_backend,
)

adaptor = detect_adaptor()
print(f"Using {adaptor} adaptor")

adaptor_flag = ADAPTOR_MAP[adaptor]
torch_flag = detect_torch_flag()
torch_backend = resolve_torch_backend(adaptor)
torch_backend_flags = list(torch_backend.compile_flags)
print(f"Using {torch_backend.name} torch backend")

plugin_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.join(plugin_dir, "..", "..")

sources = ["flagcx/src/backend_flagcx.cpp", "flagcx/src/utils_flagcx.cpp"]
include_dirs = [
    os.path.join(plugin_dir, "flagcx", "include"),
    os.path.join(repo_root, "flagcx", "include"),
    os.path.join(repo_root, "third-party", "json", "single_include"),
]

library_dirs = [
    os.path.join(repo_root, "build", "lib"),
]

libs = ["flagcx"]

# Bundle libflagcx.so into the wheel under flagcx/lib/ so the extension can
# resolve it at runtime via the $ORIGIN/lib rpath, independent of the build tree.
flagcx_lib_src = os.path.join(repo_root, "build", "lib", "libflagcx.so")
bundled_lib_dir = os.path.join(plugin_dir, "flagcx", "lib")
os.makedirs(bundled_lib_dir, exist_ok=True)
if os.path.isfile(flagcx_lib_src):
    shutil.copy2(flagcx_lib_src, os.path.join(bundled_lib_dir, "libflagcx.so"))
else:
    print(
        f"Warning: {flagcx_lib_src} not found; "
        "the wheel will not bundle libflagcx.so"
    )

# Add device-specific paths
dev_includes, dev_libdirs, dev_libs = get_device_config(
    adaptor_flag, torch_backend
)
include_dirs += dev_includes
library_dirs += dev_libdirs
libs += dev_libs

CppExtension, BuildExtension = get_ext_classes(adaptor_flag)

ext_modules = []
if CppExtension is not None:
    module = CppExtension(
        name='flagcx._C',
        sources=sources,
        include_dirs=include_dirs,
        extra_compile_args={
            'cxx': [adaptor_flag, torch_flag] + torch_backend_flags
        },
        extra_link_args=["-Wl,-rpath,$ORIGIN/lib"]
                        + [
                            "-Wl,-rpath," + d
                            for d in get_device_rpath_dirs(
                                adaptor_flag, dev_libdirs, torch_backend
                            )
                        ],
        library_dirs=library_dirs,
        libraries=libs,
    )
    ext_modules.append(module)

cmdclass = {}
if BuildExtension is not None:
    cmdclass['build_ext'] = BuildExtension

setup(
    name="flagcx",
    version="0.13.0",
    ext_modules=ext_modules,
    cmdclass=cmdclass,
    packages=find_packages() + find_packages(
        where=os.path.join(repo_root),
        include=["plugin", "plugin.*"],
    ),
    package_dir={
        "plugin": os.path.join(repo_root, "plugin"),
        "plugin.interservice": os.path.join(repo_root, "plugin", "interservice"),
    },
    package_data={"flagcx": ["lib/*.so"]},
    entry_points={"torch.backends": ["flagcx = flagcx:init"]},
)
