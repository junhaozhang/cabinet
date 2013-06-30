# -*- mode: python; -*-
# build script for Cabinet
# this requires scons
# you can get from http://www.scons.org
# then just type:
#   scons
#   sudo scons install
# to clean:
#   scons -c
# to uninstall:
#   scons -c install

import os
import platform
import sys
import SCons
import errno

EnsureSConsVersion( 1, 1, 0 )

def printLocalInfo():
    print("scons version: " + SCons.__version__)
    print("python version: " + " ".join( [ `i` for i in sys.version_info ] ))
printLocalInfo()
scons_data_dir = ".scons"
SConsignFile(scons_data_dir + "/sconsign")

# --- options ---
default_include_path = "/usr/local/include"
default_lib_path = "/usr/local/lib"
AddOption("--mute", dest = "mute", action = "store_true", default = False, help = "mute output while compiling.")
AddOption("--prefix", dest = "prefix", action = "store", default = "/usr/local", help = "Specify binary install directory.")
AddOption("--thrift-header-path", dest = "thrift-header-path", type = "string", nargs = 0, action = "store", default = default_include_path, help = "Specify thrift include header files path.")
AddOption("--thrift-lib-path", dest = "thrift-lib-path", type = "string", nargs = 0, action = "store", default = default_lib_path, help = "Specify thrift lib path.")
AddOption("--thirft-bin-path", dest = "thrift-bin-path", type = "string", nargs = 0, action = "store", default = "/usr/local/bin", help = "Specify thrift bin path.")
AddOption("--glog-header-path", dest = "glog-header-path", type = "string", nargs = 0, action = "store", default = default_include_path, help = "Specify glog header files path.")
AddOption("--glog-lib-path", dest = "glog-lib-path", type = "string", nargs = 0, action = "store", default = default_lib_path, help = "Specify glog lib path.")
AddOption("--gflags-header-path", dest = "gflags-header-path", type = "string", nargs = 0, action = "store", default = default_include_path, help = "Specify gflags header file path.")
AddOption("--gflags-lib-path", dest = "gflags-lib-path", type = "string", nargs = 0, action = "store", default = default_lib_path, help = "Specify gflags lib path.")
AddOption("--event-header-path", dest = "event-header-path", type = "string", nargs = 0, action = "store", default = "/usr/include", help = "Specify libevent header file path.")
AddOption("--event-lib-path", dest = "event-lib-path", type = "string", nargs = 0, action = "store", default = "/usr/lib", help = "Specify libevent lib path.")

if GetOption('help'):
  Return()
if GetOption('clean') and len(COMMAND_LINE_TARGETS) == 0:
  try:
    os.rmdir('build')
  except OSError as exc: # Python > 2.5
    pass

# --- environment setup ---
env = Environment(
  BUILD_DIR = '#build/',
  CONFIGUREDIR = '#' + scons_data_dir + '/sconf_temp',
  CONFIGURELOG = '#' + scons_data_dir + '/config.log'
)
if GetOption('mute'):
  env.Append(CCCOMSTR = "Compiling $TARGET")
  env.Append(CXXCOMSTR = env["CCCOMSTR"])
  env.Append(LINKCOMSTR = "Linking $TARGET")
  env.Append(ARCOMSTR = "Generating library $TARGET")

for opt in ["event-header-path", "thrift-header-path", "glog-header-path", "gflags-header-path"]:
  path = GetOption(opt)
  if not env.Dictionary().get('CPPPATH') or not (path in env["CPPPATH"]):
    env.Append(CPPPATH = [path])
env.Append(CPPPATH = [GetOption("thrift-header-path") + "/thrift"]) # thrift config.h is annoying.
for opt in ["event-lib-path", "thrift-lib-path", "glog-lib-path", "gflags-lib-path"]:
  path = GetOption(opt)
  if not env.Dictionary().get('LIBPATH') or not (path in env['LIBPATH']):
    env.Append(LIBPATH = [path])

# --- configure ---
def doConfigure(myenv):
  conf = Configure(myenv)
  if not conf.CheckCXX():
    print("c++ compiler not installed!")
    Exit(1)
  if not conf.CheckTypeSize("int", expect = 4):
    printf("int size expect 4!")
    Exit(1)
  if not conf.CheckTypeSize("long", expect = 8):
    printf("long size expect 8!")
    Exit(1)
  if not os.path.exists(GetOption("thrift-bin-path") + "/thrift"):
    printf("executable binary thrift not find!")
    Exit(1)
  if not conf.CheckCXXHeader('thrift/Thrift.h'):
    printf("thrift/concurrency/Mutex.h not include!")
    Exit(1)
  if not conf.CheckLib('thrift'):
    print("libthrift not installed!")
    Exit(1)
  if not conf.CheckLib('thriftz'):
    print("libthriftz not installed!")
    Exit(1)
  if not conf.CheckLib('thriftnb'):
    print("libthriftnb not installed!")
    Exit(1)
  if not conf.CheckCXXHeader('glog/logging.h'):
    print("glog/logging not included!")
    Exit(1)
  if not conf.CheckLib('glog'):
    print("libglog not installed!")
    Exit(1)
  if not conf.CheckCXXHeader('gflags/gflags.h'):
    print("gflags/gflags.h not include!")
    Exit(1)
  if not conf.CheckLib('gflags'):
    print("libgflag not installed!")
  if not conf.CheckLib('event'):
    print("libevent not installed!")

doConfigure(env)

# --- build ---
# mkdir
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python > 2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise
build_abs_path = env.Dir("$BUILD_DIR").abspath
mkdir_p(build_abs_path)

# thrift
"""
env.Append(BUILDERS = {'Thrift' :
  env.Builder(
    action = [ "%s/thrift -gen cpp -o %s $SOURCE" % (GetOption("thrift-bin-path"), env.Dir("$BUILD_DIR").abspath) ]
  )
})
"""
# ref: http://www.scons.org/wiki/DynamicSourceGenerator
thriftgenlist = env.Command(
  source = "cabinet.thrift",
  target = "$BUILD_DIR/gen-cpp/source.list",
  action = "%s/thrift -gen cpp -o %s $SOURCE && (ls -1 %s/gen-cpp/*.cpp | sort > $TARGET)" % (GetOption("thrift-bin-path"), build_abs_path, build_abs_path)
)
env.AlwaysBuild(thriftgenlist)

def scansrcs(target, source, env):
  """Scans through the list in the file 'source', calling
    env['SCANSRCS_FUNC'](env, line) on each line of that file.
    Calls env['SCANSRCS_PREFUNC'](env, source) once before scanning.
    Calls env['SCANSRCS_POSTFUNC'](env, source) once finish scanning.
  """
  if type(source) is list:
    source = source[0]
  if 'SCANSRCS_FUNC' not in env:
    raise Error, "You must define SCANSRCS_FUNC as a scons env function."
  # Call the pre-func
  if 'SCANSRCS_PREFUNC' in env:
    env['SCANSRCS_PREFUNC'](env, source.path)

  f = None
  try:
    f = open(source.path, 'r')
  except:
    print "scansrcs: Can't open source list file '%s' in %s" % (source.path, os.getcwd())
    raise
  # Scan through the lines
  for line in f.readlines():
    src = line.strip()
    try:
      env['SCANSRCS_FUNC'](env, src)
    except:
     print "SCANSRCS func raised exception:"
     raise
    f.close()
  if 'SCANSRCS_POSTFUNC' in env:
    env['SCANSRCS_POSTFUNC'](env, source.path)
# This is a funky bulder, because it never creates its target.
# Should alwasy be called with a fake target name.
env.Append(BUILDERS = {'ScanSrcs' : env.Builder(action = scansrcs)})

def add_thrift_gen_source(env, source):
  env.Append(THRIFT_GEN_SOURCE = [source])

def finish_add_thrift_gen_source(env, dummy):
  env.Library(source = env['THRIFT_GEN_SOURCE'], target = "$BUILD_DIR/cabinet_thrift_gen", CPPDEFINES=["HAVE_NETINET_IN_H"])
  env['THRIFT_GEN_SOURCE'] = []

scansrcs = env.ScanSrcs("#BUILD_DIR/gen-cpp/dummy", thriftgenlist, SCANSRCS_FUNC = add_thrift_gen_source, SCANSRCS_POSTFUNC = finish_add_thrift_gen_source)
env.AlwaysBuild(scansrcs)

# cabinet server
env.Append(CPPPATH = ['$BUILD_DIR'])
cabineto = env.Object(
  source = 'CabinetServer.cc',
  target = '$BUILD_DIR/cabineto',
  CPPDEFINES = [ "HAVE_CONFIG_H" ]
)
env.Depends(cabineto, thriftgenlist)
cabinetd = env.Program(
  source = cabineto,
  target = '$BUILD_DIR/cabinetd',
  LIBPATH = ['$BUILD_DIR'],
  LIBS = [ 'thrift', 'thriftz', 'thriftnb', 'gflags', 'glog', 'cabinet_thrift_gen', 'event' ],
  CPPDEFINES = [ "HAVE_CONFIG_H" ]
)
env.Depends(cabinetd, [scansrcs, "$BUILD_DIR/libcabinet_thrift_gen.a"])
env.Default(cabinetd)

# cabinet server
# --- install ---
env.Alias("install",
  [
    env.Install(GetOption("prefix") + "/include", "cabinet.thrift"),
    env.Install(GetOption("prefix") + "/bin", cabinetd)
  ]
)
