from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

VERSION = '0.0.1'

setup(
    name = "gevent-memcache",
    version = VERSION,
    license = "New BSD",
    description = "A gevent (http://www.gevent.org) adaption of the asynchronous Memcache from the Concurrence framework (http://opensource.hyves.org/concurrence)",
    cmdclass = {"build_ext": build_ext},
    package_dir = {'':'lib'},
    packages = ['geventmemcache'],
    ext_modules = [Extension("geventmemcache.common", ["lib/geventmemcache/geventmemcache.common.pyx"])]
)