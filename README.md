# MedusaStorage

This gem exists to hide some of the interaction with underlying storage mechanisms
(e.g. a filesystem or S3) so that client code can operate on the content in a 
uniform manner.

The basic concept is a 'root'. This specifies a location from which content can be 
addressed by a 'key'. For example, the root might be a directory on a filesystem
and the key the relative path from that directory. Or it might be a bucket and 
(possibly empty) prefix in S3, and the medusa_storage key specifies the rest of the  
Amazon key via bucket:prefix/key.

Once we have the root/key pair, we can do various things like find out if it really
exists in storage, get information about it, or get an IO or true file from it. We
will continue to build these capabilities as we need them.

Additionally, we can have a set of roots so that an application can talk to different
storages or different points in a storage by setting up appropriate roots.

Note that we organize our S3 content using a directory-ish structure, i.e. with
slashes in the keys indicating how we want to think of things as directories and
files. 

## Usage

Install the gem in the usual way in the Gemfile. You'll need to use the git: argument
to point to the github repository.

Typically the gem will be used by instantiating and storing a MedusaStorage::RootSet.
To configure, pass an array where each entry is a hash with symbol 
keys to configure a single root. For all roots the hash will have a name: key that 
should be unique in the RootSet. The root can be extracted from the RootSet using
the 'at' method with the name. It also has a type: key that expresses what type 
of storage the root is. 

A 'filesystem' type root has a path: key that gives the path on the filesystem that is 
used as the base of that root's storage. Key arguments to the methods then reflect
a relative path starting there.

An 's3' type root has keys bucket:, aws_access_key_id:, aws_secret_access_key, and region: 
that take the appropriate Amazon S3 configuration information. There is also an optional
prefix: key that allows us to specify a prefix inside the bucket that is prepended to
all key arguments to the various methods. By default this is blank.

There are a set of methods common to the various types of roots as well as some
specific to a particular one. For example, you can use 'size' with any root, but 
'presigned_get_url' only really makes sense with an S3 root.

I'm not going to document all of these at this time - check the code. I will be 
adding more as the need for them arise. Some of them 
work with a particular object, e.g. to get its size. Others do things like get all 
of the file keys or directory keys 'in' a directory, or the entire set of keys 'under'
a directory (these are useful to use, for example, in generating downloader manifests). 
Others deliver the content at a key as either an IO or a string path to
a file, to be yielded to a block (the former is useful for generating fixity checksums,
the latter for FITS, which doesn't operate nicely on just a stream). 
See the individual method documentation.

##TODO

Just jotting down some things that need attention that I'm skipping over to get the
broad outlines in place.

* methods to copy/move within a root and to copy from one root to another
* Metadata beyond mtime and md5_sum. Send as headers to S3 or set on filesystem as appropriate
  and possible. I'm not sure if there is anything else we're really keeping track of here.
* Tests, of course. They'd be particularly valuable here.
 

