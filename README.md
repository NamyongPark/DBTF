# DBTF

> Fast and Scalable Method for Distributed Boolean Tensor Factorization.<br/>
> [Namyong Park](mailto:namyongp@cs.cmu.edu) (main contact), [Sejoon Oh](mailto:sejun6431@gmail.com), and [U Kang](mailto:ukang@snu.ac.kr).<br/>
> VLDB Journal, 2019 / ICDE 2017.
>
> Version: 2.0<br/>
> Date: Jan 30, 2019<br/>

### 1. General information

DBTF (Distributed Boolean Tensor Factorization) is a distributed method for
Boolean CP (DBTF-CP) and Tucker (DBTF-TK) factorizations running on the Spark framework.
Given a binary input tensor, DBTF performs Boolean CP or Tucker factorization on it,
and outputs binary factor matrices, and a core tensor (for Tucker factorization).
Please check the [project page](https://www.cs.cmu.edu/~namyongp/dbtf/) for more details.

### 2. How to run DBTF

#### IMPORTANT!
- Before you use it, please check that the script files are executable. If not,
you may manually modify the permission of scripts or you may type "make install"
to do the same work.
- You may type "make demo" to try DBTF on a sample tensor.
- Apache Spark should be installed in your system.

#### To run DBTF, you need to do the followings:
- Prepare a binary input tensor file.
    - The binary input tensor should be stored in a sparse tensor format as a plain text file
  where each line is in "MODE1-INDEX SEPARATOR MODE2-INDEX SEPARATOR MODE3-INDEX" format.
  You can use a comma (csv, default), a space (ssv), or a tab (tsv) as a separator, and
  specify the separator of an input tensor file as an option.
  You can specify the base (start) index of an input tensor by using --base-index parameter, which should be either 0 or 1.
  For a full list of available parameters, please see the usage below.

    - For example, these are the first five lines of an example tensor file "sample.tensor."

          22,32,28
          23,29,42
          10,15,39
          17,8,49
          3,7,30
          .....

- Execute ``./dbtf.sh``
```
Usage: dbtf.sh [options] <tensor-file-path>

    -m <value> | --factorization-method <value>
          factorization method (cp or tk)
    -b <value> | --base-index <value>
          base (start) index of a tensor (should be either 0 or 1)
    -r <value> | --rank <value>
          rank (required for CP)
    -r1 <value> | --rank1 <value>
          rank for the first factor (required for Tucker)
    -r2 <value> | --rank2 <value>
          rank for the second factor (required for Tucker)
    -r3 <value> | --rank3 <value>
          rank for the third factor (required for Tucker)
    -rs <value> | --random-seed <value>
          random seed
    -m1 <value> | --mode1-length <value>
          dimensionality of the 1st mode
    -m2 <value> | --mode2-length <value>
          dimensionality of the 2nd mode
    -m3 <value> | --mode3-length <value>
          dimensionality of the 3rd mode
    -nic <value> | --num-initial-candidate-sets <value>
          number of initial sets of factor matrices (and a core tensor) (default=1)
    -ifd <value> | --initial-factor-matrix-density <value>
          density of an initial factor matrix
    -icd <value> | --initial-core-tensor-density <value>
          density of an initial core tensor (required for Tucker)
    -cni <value> | --converge-by-num-iters <value>
          marks the execution as converged when the number of iterations reaches the given value (default=10). this is the default method used for checking convergence.
    -ced <value> | --converge-by-abs-error-delta <value>
          marks the execution as converged when the absolute error delta between two consecutive iterations becomes less than the given value.
    -nci <value> | --num-consecutive-iters-for-converge-by-abs-error-delta <value>
          when --converge-by-abs-error-delta is set, marks the execution as converged when the absolute error delta was smaller than the specified threshold value for the given number of consecutive iterations (default=3)
    -pz <value> | --prob-for-zero-for-tie-breaking <value>
          probability to choose zero when several column values (including zero) have the same error delta (default=0.9)
    -mz <value> | --max-zero-percentage <value>
          maximum percentage of zeros in a factor matrix (default=0.95)
    -mrss <value> | --max-rank-split-size <value>
          maximum number of rows out of which to build a single cache table (default=15).
    -up <value> | --num-unfolded-tensor-partitions <value>
          number of partitions of unfolded tensors
    -ip <value> | --num-input-tensor-partitions <value>
          number of partitions of the input tensor (required for Tucker)
    -ce <value> | --compute-error <value>
          computes the reconstruction error for every iteration (every) or only for the end iteration (end)
    -od <value> | --output-dir-path <value>
          path to an output directory (if missing, log messages will be printed only to stdout)
    -op <value> | --output-file-prefix <value>
          prefix of an output file name
    --tensor-file-separator-type <value>
          separator type of the input tensor file: ssv, tsv, or csv (default: csv)
    --help
          prints this usage text
    --version
          prints the DBTF version
    <tensor-file-path>
          input tensor file path
  if --converge-by-num-iters and --converge-by-abs-error-delta are given together, the execution will stop when either of the convergence conditions is satisfied
```

You can try DBTF by running `demo-cp.sh` and `demo-tk.sh`,
which execute DBTF-CP and DBTF-TK, respectively, on a sample binary tensor (sample.tensor).

### 3. Citing

If you use DBTF for your research, please cite the following papers:

    @inproceedings{dbtf-vldbj2019,
      author    = {Namyong Park and Sejoon Oh and U. Kang},
      title     = {Fast and scalable method for distributed Boolean tensor factorization},
      journal = {{VLDB} J.},
      year      = {2019}
    }

    @inproceedings{dbtf-icde2017,
      author    = {Namyong Park and Sejoon Oh and U. Kang},
      title     = {Fast and Scalable Distributed Boolean Tensor Factorization},
      booktitle = {33rd {IEEE} International Conference on Data Engineering, {ICDE} 2017, San Diego, CA, USA, April 19-22, 2017},
      pages     = {1071--1082},
      year      = {2017}
    }