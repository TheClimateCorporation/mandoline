# mandoline-core

Distributed, versioned, n-dimensional array database.

[![Build Status](https://travis-ci.org/TheClimateCorporation/mandoline.svg?branch=master)](https://travis-ci.org/TheClimateCorporation/mandoline)

What is Mandoline?
==================

Mandoline is a Clojure library for reading and writing immutable,
versioned datasets that contain multidimensional arrays. The Mandoline
library can be extended to use different data store
implementations. Currently supported data store implementations are:

- an in-memory local data store using Clojure atoms
- a [local filesystem store](https://github.com/TheClimateCorporation/mandoline-sqlite)
  using the [SQLite](http://www.sqlite.org/) transactional database library
- a [distributed data store](https://github.com/TheClimateCorporation/mandoline-dynamodb)
  using the AWS [DynamoDB](http://aws.amazon.com/dynamodb/) service

Usage
=====

If your project uses Leiningen, then it's as simple as sticking the
following in your project.clj's :dependencies section:

[![Clojars Project](http://clojars.org/io.mandoline/mandoline-core/latest-version.svg)](http://clojars.org/io.mandoline/mandoline-core)

Please note that this will only give you the in-memory store. For a
persistent store, use one of the options listed above.

Tutorial
========

Overview
--------

This tutorial will walk you through:

- The concept of *metadata* in Mandoline
- The concept of a data *slab* in Mandoline
- Creating a new dataset
- Writing data to a new dataset
- Reading data from a dataset
- Writing data to a new version of a dataset
- Reading data from multiple versions of a dataset
- Deleting a dataset

For this tutorial, you need to start a Clojure REPL in the
`io.mandoline/mandoline-core` project or in a project that includes
`io.mandoline/mandoline-core` as a dependency.

Start the REPL and require/import the following:

        user=> (require '[io.mandoline :as mandoline])
        nil
        user=> (require '[io.mandoline.dataset :as dataset])
        nil
        user=> (require '[io.mandoline.slab :as slab])
        nil
        user=> (require '[io.mandoline.slice :as slice])
        nil
        user=> (require '[io.mandoline.impl :as impl])
        nil
        user=> (import '[ucar.ma2 Array])
        ucar.ma2.Array

A Mandoline dataset loosely resembles a
[NetCDF](http://www.unidata.ucar.edu/software/netcdf/) or [Common Data
Model](https://www.unidata.ucar.edu/software/thredds/current/netcdf-java/CDM/)
dataset. A dataset contains zero or more *variables* (arrays) that are defined on
named *dimensions* (array axes). Each variable is a (possibly
multi-dimensional) array of homogeneous type that is defined on zero or
more dimensions. Multiple variables can share dimensions.

To create a Mandoline dataset, you need to provide:

1. a *metadata map* that defines the structure of the dataset, and
2. *slabs* that contain array values to populate the variables

These ingredients will be described in the next two parts of this
tutorial.

Metadata
--------

As an example, define the following metadata map in the REPL (adapted
from a real-world [netCDF
dataset](http://www.unidata.ucar.edu/software/netcdf/examples/ECMWF_ERA-40_subset.cdl)):

        (def metadata
          {:dimensions
           {:longitude 144, :latitude 73, :time 62}
           :chunk-dimensions
            {:longitude 20, :latitude 20, :time 40}
           :variables
           {:longitude
            {:type "float"
             :fill-value Float/NaN
             :shape ["longitude"]}
            :latitude
            {:type "float"
             :fill-value Float/NaN
             :shape ["latitude"]}
            :time
            {:type "int"
             :fill-value Integer/MIN_VALUE
             :shape ["time"]}
            :tcw
            {:type "short"
             :fill-value Short/MIN_VALUE
             :shape ["time" "latitude" "longitude"]}}})

This metadata map describes a dataset that has this structure:

* Dimensions
      - `longitude`: length is `144`, and storage chunk size is `20`
      - `latitude`: length is `73`, and storage chunk size is `20`
      - `time`: length is `62`, and storage chunk size is `40`
* Variables
      - `longitude`: 1-dimensional array of type `float` with shape
        `[144]`, defined on the `longitude` dimension
      - `latitude`: 1-dimensional array of type `float` with shape
        `[73]`, defined on the `latitude` dimension
      - `time`: 1-dimensional array of type `int` with shape `[62]`,
        defined on the `time` dimension
      - `tcw`: 3-dimensional array of type `short` with shape `[62 73
        144]`, defined on the dimensions `[time latitude longitude]`

You may wonder what is the significance of the `:chunk-dimensions` entry
in the metadata map. It can be regarded as a leaky implementation detail
or a hint to the underlying data store for Mandoline. Each variable is
partitioned into non-overlapping tiles ("chunks") whose maximum extent
along each dimension is specified by `:chunk-dimensions`.

You may also wonder what is the significance of the `:fill-value` entry
that is associated with each variable in the metadata map. Mandoline
requires a default element value for each variable so that it can
optimize storage. This default value is specified by `:fill-value` and
is mandatory.

You can use the function
`io.mandoline.dataset/validate-dataset-definition` to check
whether a metadata map is well-formed. This function throws an
exception on invalid metadata and otherwise returns nil.

        user=> (dataset/validate-dataset-definition metadata)
        nil

Slab
----

Now you have a metadata map that describes the structure of the dataset.
You also need data to populate the dataset. The Mandoline library
enables you to write data to a contiguous section of a single variable,
which is called a "slab". The namespace `io.mandoline.slab`
defines a `Slab` record type. A `Slab` record has two fields

1. The (possibly multi-dimensional) array data to be written, which must
   be a
   [`ucar.ma2.Array`](https://www.unidata.ucar.edu/software/thredds/current/netcdf-java/v4.0/javadoc/ucar/ma2/Array.html)
   instance. The data type of this array data must match the data type
   of the destination variable.
2. The ranges of array indices that specify *where* in the destination
   variable the array data is to be written, which must be an instance
   of the `io.mandoline.slice/Slice` record type. You can use
   the convenience function `io.mandoline.slice/mk-slice` to
   create a `Slice` instance. The slice must be compatible with the
   shape of the destination variable.

As an example, create a 1-dimensional slab with shape `[10]` that
corresponds to the index range from 0 (inclusive) to 10 (exclusive) of a
variable:

        user=> (let [array (Array/factory Float/TYPE (int-array [10]))
          #_=>       slice (slice/mk-slice [0] [10])]
          #_=>   (slab/->Slab array slice))
        #io.mandoline.slab.Slab{:data #<D1 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 >, :slice #io.mandoline.slice.Slice{:start [0], :stop [10], :step [1]}}

Note that this slab is defined independently of any variable in a
Mandoline dataset. It contains array values that can *potentially* be
assigned to a subsection of a variable, but it does not inherently
represent an assignment operation. If you were to attempt to write this
slab to a specific variable in a dataset (as you will do later in this
tutorial), Mandoline would fail the attempted write if any of the
following conditions were not satisfied:

- The destination variable has data type `"float"` to match the data
  type of the slab (`Float/TYPE`).
- The destination variable is 1-dimensional, to match the 1-dimensional
  data in the slab.
- The destination variable has an extent that is long enough so that
  indices from 0 (inclusive) to 10 (exclusive) along its 0th dimension
  are valid indices.

To populate a large variable, you will need to perform distributed
writes with multiple slabs, where each slab fits in the memory of a
single process but the collection of all slabs is prohibitively large.
The Slab write interface of Mandoline is designed to support this use
case.

To successfully write to a variable, a slab does not need to coincide
with the chunks that are defined by `:chunk-dimensions` in the dataset's
metadata map. Mandoline automatically partitions a slab into (possibly
partial) chunks for storage.

Mandoline uses the `Slab` record type for reading data as well as for
writing. The function `io.mandoline/get-slice` (which you will
use later in this tutorial) returns a `Slab` instance.

To continue this tutorial, define the following slabs to write to your
sample dataset:

        (def slabs
          {:longitude
           [(slab/->Slab
              (Array/factory
                Float/TYPE
                (int-array [144])
                (float-array (range 0 360 2.5)))
              (slice/mk-slice [0] [144]))]
           :latitude
           [(slab/->Slab
              (Array/factory
                Float/TYPE
                (int-array [73])
                (float-array (range 90 -92.5 -2.5)))
              (slice/mk-slice [0] [73]))]
           :time
           [(slab/->Slab
              (Array/factory
                Integer/TYPE
                (int-array [62])
                (int-array (range 898476 899214 12)))
              (slice/mk-slice [0] [62]))]
           :tcw
           [(slab/->Slab
              (Array/factory
                Short/TYPE
                (int-array [62 73 144])
                (short-array
                  (repeatedly
                    (* 62 73 144)
                    #(short (rand-int Short/MAX_VALUE)))))
              (slice/mk-slice [0 0 0] [62 73 144]))]})

This `slabs` var is a map whose keys are variable keywords and whose
values are data slabs. Because the example dataset is small, you can
populate each variable with one slab that covers the entire extent of
the variable. However, keep in mind that you are writing a *collection*
of slabs to each variable; it is an arbitrary coincidence that each
collection has a size of 1.

Creating a new dataset
----------------------

Now you have a metadata map that describes the structure of a dataset
and data slabs that you can use to populate this dataset. Generate a
unique name for the new dataset that you are about to create

        user=> (def dataset-name
          #_=>   (apply str (repeatedly 6 #(rand-nth "ABCDEFGHIJKLMNOPQRSTUVWXYZ"))))
        #'user/dataset-name

and specify a *root table* for your dataset

        user=> (def root-table "integration-testing.mandoline.io")
        #'user/root-table

The root table is a prefix for grouping multiple datasets; you don't
need to fully understand it for this tutorial.

The root table and the dataset name can be combined into a single
Mandoline URI:

        user=> (def uri (format "ddb://%s/%s" root-table dataset-name))
        #'user/uri
        user=> uri
        "ddb://integration-testing.mandoline.io/KNREFI"

The schema for this URI is `ddb://`, which means that you are using the
DynamoDB storage backend for Mandoline. Mandoline also has an in-memory
storage backend, which uses the URI scheme `mem://`.

This dataset URI is for the convenience of human readers only. The
Mandoline library parses it to an equivalent map that is called a dataset
spec:

        user=> (def spec (impl/mk-store-spec uri))
        #'user/spec
        user=> (pprint spec)
        {:store "ddb",
         :db-version nil,
         :root "integration-testing.mandoline.io",
         :dataset "KNREFI"}
        nil

Most of the functions in the `io.mandoline` namespace operate on
a spec map or a map that is derived from a spec.

To create a dataset, call the function `io.mandoline/create` on
the dataset spec:

        user=> (mandoline/create spec)
        nil

Calling this function requires AWS credentials to interact with
DynamoDB. This function has side effects on the backend store and
returns nil. If you watch the
[DynamoDB Tables](https://console.aws.amazon.com/dynamodb/home) in the
AWS Console, you can see new tables being created. It is also
idempotent.  The first time you call it, it may take a while to
return, because it has to poll DynamoDB. Subsequent calls ought to
return more quickly.

At this point, you have a Mandoline dataset that is empty and that has
no version history. To write to this dataset, call the function
`io.mandoline/dataset-writer` on the dataset spec:

        user=> (def writer (mandoline/dataset-writer spec))
        #'user/writer
        user=> (pprint writer)
        {:chunk-store
         #<CachingChunkStore io.mandoline.impl.cache.CachingChunkStore@235d74ea>,
         :dataset-spec
         {:store "ddb",
          :db-version nil,
          :root "integration-testing.mandoline.io",
          :dataset "KNREFI"},
         :schema
         #<DynamoDBSchema io.mandoline.impl.dynamodb.DynamoDBSchema@4ca6c0c9>,
         :connection
         #<DynamoDBConnection io.mandoline.impl.dynamodb.DynamoDBConnection@57baf36f>}
        nil

The `dataset-writer` function returns a "writer" map whose keys are
`(:dataset-spec :schema :connection :chunk-store)`. This map contains
everything that Mandoline uses to write to a dataset. The
`:dataset-spec` entry is simply the dataset spec that you provided. The
other entries are objects that implement Mandoline protocols, which are
defined in the namespace `io.mandoline.impl.protocol`.

Specifically,

- The `:schema` entry implements the Mandoline `Schema` protocol. A
  schema can be loosely considered as the "parent" of zero or more
  Mandoline datasets.
- The `:connection` entry implements the Mandoline `Connection`
  protocol. A connection is an interface to a single dataset (including
  the version history of the dataset).
- The `:chunk-store` entry implements the Mandoline `ChunkStore`
  protocol. A chunk store is an interface to the byte-level storage of
  the array chunks that comprise variables in a dataset.

You can interact with the `Schema` instance that corresponds to your new
dataset. To list of all datasets that are defined under this schema,
call its `list-datasets` method, which returns a set of dataset names,
including the name of the new dataset that you just created:

        user=> (:schema writer)
        #<DynamoDBSchema io.mandoline.impl.dynamodb.DynamoDBSchema@4ca6c0c9>
        user=> (type (.list-datasets (:schema writer)))
        clojure.lang.PersistentHashSet
        user=> (contains? (.list-datasets (:schema writer)) dataset-name)
        true

You can also interact with the `Connection` instance that corresponds to
your new dataset. To list all versions of this dataset, call its
`versions` method, which returns a seq. (This method takes a second
argument; you can safely provide an empty map.) For the dataset that you
just created, no versions exist, so the `version` method returns an
empty list.

        user=> (:connection writer)
        #<DynamoDBConnection io.mandoline.impl.dynamodb.DynamoDBConnection@57baf36f>
        user=> (.versions (:connection writer) {})
        ()

The `get-stats` method returns storage statistics for the dataset. As
expected, the new dataset uses zero storage:

        user=> (.get-stats (:connection writer))
        {:metadata-size 0, :index-size 0, :data-size 0}

Writing data to a dataset
-------------------------

Now you are ready to write slabs of data to variables in this dataset.
Do the following in the REPL:

        (def new-version
          (let [version-writer (mandoline/add-version writer metadata)]
            (doseq [v (keys (:variables metadata))]
              (with-open [w (mandoline/variable-writer version-writer v)]
                (mandoline/write w (v slabs))))
            (mandoline/finish-version version-writer)))

This will take a while to run. There is a lot going on here. Step by step:

1. In the `let` binding, the function `io.mandoline/add-version`
   is called on two arguments, the dataset writer map (which was
   returned by `io.mandoline/dataset-writer`) and the metadata
   map (which you defined earlier in this tutorial.) The `add-version`
   function associates necessary version information to the dataset
   writer map. Every time you write a new version of a dataset, you need
   to call the `add-version` function and use the version-aware dataset
   writer map that is returned.
2. The `doseq` form iterates over variables in the metadata map. Recall
   that metadata map has variable keywords `(:longitude :latitude :time
   :twc)`. For each variable keyword, the function
   `io.mandoline/variable-writer` is called to create a variable
   writer for that variable. Each variable writer, along with the data
   slabs that correspond to the variable, is passed to the function
   `io.mandoline/write`, which writes a sequence of slabs to a
   variable in a dataset. The `variable-writer` function returns an
   object that implements `java.io.Closeable`, so the `with-open` macro
   automatically closes it when finished.
3. Finally, the function `io.mandoline/finish` is called on the
   dataset writer map. This function "commits" a new version of the
   dataset and returns an identifier (a long) for this new version.

After the writes are finished and the new version is committed, you can
use the `Connection` protocol to check that the version exists.

        user=> (count (.versions (:connection writer) {}))
        1
        user=> (= (str new-version) (:version (first (.versions (:connection writer) {}))))
        true

Reading data from a dataset
---------------------------

Now that you have created a new dataset and populated it with data, you
can read it. The function `io.mandoline/dataset-reader` returns
a dataset reader map, which looks similar to the writer map that you
were just using.

        user=> (def reader (mandoline/dataset-reader spec))
        #'user/reader

Try to read the last 10 elements of the `:time` variable. To do this, you
need to construct a `Slice` instance:

        user=> (def request-slice
          #_=>   (let [upperbound (get-in metadata [:dimensions :time])
          #_=>         lowerbound (- upperbound 10)]
          #_=>     (slice/mk-slice [lowerbound] [upperbound])))
        #'user/request-slice
        user=> request-slice
        #io.mandoline.slice.Slice{:start [52], :stop [62], :step [1]}

Using the dataset reader and the request slice, you can do the following
to get a corresponding slab of array data from the `:time` variable of
the *latest* version of the dataset:

        (-> reader
          (mandoline/on-last-version)
          (mandoline/variable-reader :time)
          (mandoline/get-slice request-slice))

There is a lot going on here. Step by step:

1. The function `io.mandoline/on-last-version` uses the
   `Connection` protocol to look up the latest version of the dataset and
   associates this version information with the dataset reader map.
2. The function `io.mandoline/variable-reader` constructs a
   single-variable reader map. In this case, the specified variable is
   `:time`.
3. The function `io.mandoline/get-slice` takes a variable reader
   and a slice and returns the corresponding slab of array data from the
   variable.

Writing data to a new version of a dataset
------------------------------------------

At this point in the tutorial, you have created a new dataset, populated
and committed the first version of this dataset, and read data from the
version that you commited. Now you will append another version to the
version history of this dataset.

Create a 2-element collection of 1-by-1-by-1 slabs to overwrite opposite
corners of the `:tcw` variable.

        (def overwrite-slabs
           [(slab/->Slab
              (Array/factory
                Short/TYPE
                (int-array [1 1 1])
                (short-array (map short [0])))
              (slice/mk-slice [0 0 0] [1 1 1]))
            (slab/->Slab
              (Array/factory
                Short/TYPE
                (int-array [1 1 1])
                (short-array (map short [0])))
              (slice/mk-slice [61 72 143] [62 73 144]))])

You can reuse the dataset writer map that you created earlier, as long
as you call the functions `on-last-version` and `add-version` on it to
update the map:

        (def new-new-version
          (let [version-writer (-> writer
                                 (mandoline/on-last-version)
                                 (mandoline/add-version metadata))]
            (with-open [w (mandoline/variable-writer version-writer :tcw)]
              (mandoline/write w overwrite-slabs))
            (mandoline/finish-version version-writer)))

After doing this, you can verify that there are now 2 versions:

        user=> (count (.versions (:connection writer) {}))
        2
        user=> (= (str new-new-version) (:version (first (.versions (:connection writer) {}))))
        true

The `versions` method of the `Connection` protocol returns versions in
reverse-chronological order, so that the newest version is listed first.

Reading data from multiple versions of a dataset
------------------------------------------------

Now you have a dataset with 2 versions in its history. In this section,
you will read discrepant data from different versions.

Using the function `io.mandoline/on-version`, you can specify a
specific dataset version by its version identifier. To get array values
from a corner from `:tcw` variable in the first version of the dataset,
do

        user=> (-> reader
          #_=>   (mandoline/on-version (str new-version))
          #_=>   (mandoline/variable-reader :tcw)
          #_=>   (mandoline/get-slice (slice/mk-slice [0 0 0] [1 1 1]))
          #_=>   (:data))
        #<D3 27150 >

To get array values from the same corner of the same variable in the
second version of the dataset (after it was overwritten with zero), do

        user=> (-> reader
          #_=>   (mandoline/on-version (str new-new-version))
          #_=>   (mandoline/variable-reader :tcw)
          #_=>   (mandoline/get-slice (slice/mk-slice [0 0 0] [1 1 1]))
          #_=>   (:data))
        #<D3 0 >


Deleting a dataset
------------------

*WARNING*: Current implementations of Mandoline provide no safeguards
for recovery of deleted data. Be certain that you are deleting the
correct dataset when you perform this section of the tutorial.

In the final section of this tutorial, you will delete the dataset that
you created. You can call the `destroy-dataset` method of the `Schema`
instance that corresponds to the dataset.

        user=> (.destroy-dataset (:schema writer) dataset-name)
        nil
        user=> (contains? (.list-datasets (:schema writer)) dataset-name)
        false

Subsequent attempts to read from the dataset will trigger an exception:

        user=> (-> reader
          #_=>   (mandoline/on-last-version)
          #_=>   (mandoline/variable-reader :time)
          #_=>   (mandoline/get-slice request-slice))
        ResourceNotFoundException Requested resource not found  com.amazonaws.http.AmazonHttpClient.handleErrorResponse (AmazonHttpClient.java:644)

Credits
-------
Shoutouts to Brian Davis, Alice Liang, Steve Kim, and Sebastian Galkin for
being major contributors to this project.  More shoutouts to Jeffrey
Gerard, Tim Chagnon, Satshabad Khalsa, Daniel Richman, Arthur Silva, and
Leon Barrett for contributing to Mandoline.
