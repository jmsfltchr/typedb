rebuild() {
  curdir=`pwd`
  WORKHOME=/Users/jamesfletcher/programming

  cd $WORKHOME/grakn
  echo "Starting Bazel distribution rebuild"
  bazel build //:assemble-mac-zip

  if [ -d $WORKHOME/dist/grakn-core-all-mac ]
  then
    rm -rf $WORKHOME/dist/grakn-core-all-mac
  fi
  echo "Unzipping new server"
  unzip $WORKHOME/grakn/bazel-bin/grakn-core-all-mac.zip -d $WORKHOME/dist


  if [ "$1" = "debug" ]
  then
    echo "Starting new server in debug "
        SERVER_JAVAOPTS='-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005' $WORKHOME/dist/grakn-core-all-mac/grakn server
  else
    echo "Starting new server"
    $WORKHOME/dist/grakn-core-all-mac/grakn server
  fi
  cd $curdir
}