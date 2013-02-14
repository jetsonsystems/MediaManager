#/bin/sh

for DIR in \
	MediaManagerAppConfig \
	MediaManagerStorage \
	ImageService \
	MediaManagerApi \
	MediaManagerAppSupport
do
	pushd $DIR
	npm install --link 
	npm link
	popd
done
