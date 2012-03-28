============
Introduction
============


What is pypeFLOW
================

pypeFLOW is light weight and reusable make / flow data process
library written in Python.

Most of bioinformatics analysis or general data analysis
includes various steps combining data files, transforming
files between different formats and calculating statistics
with a variety of tools. Ian Holmes has a great summary and
opinions about bioinformatics workflow at
http://biowiki.org/BioinformaticsWorkflows.  It is
interesting that such analysis workflow is really similar to
constructing software without an IDE in general.  Using a
"makefile" file for managing bioinformatics analysis
workflow is actually great for generating reproducible and
reusable analysis procedure.  Combining with a proper
version control tool, one will be able to manage to work
with a divergent set of data and tools over a period of time
for a project especially when there are complicate
dependence between the data, tools and customized code
for the analysis tasks.

However, using "make" and "makefile" implies all data
analysis steps are done by some command line tools. If you
have some customized analysis tasks, you will have to write
some scripts and to make them into command line tools.  In
my personal experience, I find it is convenient to bypass
such burden and to combine those quick and simple steps in a
single scripts. The only caveat is that if an analyst does
not save the results of any intermediate steps, he or she
has to repeat the computation all over again for every steps
from the beginning. This will waste a lot of computation
cycles and personal time.  Well, the solution is simple,
just like the traditional software building process, one
have to track the dependencies and analyze them and only
reprocess those parts that are necessary to get the most
up-to-date final results.

General Design Principles
=========================

    - Explicitly modeling data and task dependency
    - Support declarative programming style within Python while
      maintaining something that imperative programming dose the
      best
    - Utilize RDF meta-data framework
    - Keep it simple if possible

Features
========

    - Multiple concurrent tasks scheduling and running
    - Support tasks as simple shell script (considering clustering
      job submission in mind)
    - reasonable simple interface for declarative programming

