# /usr/bin/python

env = Environment()

library = env.Library('core/Cabinet.cc', CPPPATH = ['.'])

# targets
# TODO(junhaozhang): targets install, doc, lint, client, server, all...
env.Alias("library", library)
env.Default("library")
