from setuptools import setup
from setuptools import Extension

mod = Extension(
        '_linux_aio',
        sources=['_linux_aio.c'],
        extra_compile_args=['-Wall', "-O2"],
        )

setup(
    name='linux_aio',
    ext_modules=[mod]
)
