Put the following frameworks here as either a symlink or simply a copy after you have built them:

  * CouchCocoa.framework:

    You will need to build it. See: https://github.com/couchbaselabs/CouchCocoa.

    After you build it, you can copy or symlink as follows (using the actual path):

      ln -s ~/Library/Developer/Xcode/DerivedData/CouchCocoa-gbdtbtbssoyknzdrdrjauwdpqzmd/Build/Products/Debug/CouchCocoa.framework

  * TouchDB.framework:

    You will need to build it. See: https://github.com/couchbaselabs/TouchDB-iOS

    After you build it, you can copy or symlink as follows (using the actual path:

      ln -s ~/Library/Developer/Xcode/DerivedData/TouchDB-fracyogmzhnxdwfbagcppkufpjsx/Build/Products/Debug/TouchDB.framework .

  * TouchDBListener.framework:

    It will be built as part of TouchDB-iOS.

    After you build it, you can copy or symlink as follows:

      ln -s ~/Library/Developer/Xcode/DerivedData/TouchDB-fracyogmzhnxdwfbagcppkufpjsx/Build/Products/Debug/TouchDBListener.framework
