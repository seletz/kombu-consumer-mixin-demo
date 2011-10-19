======================================
Projekt: Kombu Consumer Mixin Research
======================================

:Author: Stefan Eletzhofer
:Date:   2011-10-19

Abstract
========

The goal of this project is to test out a plan to move from **pika** to 
**kombu** in the `nexiles.settr.export` project.

The system designes is a very simple job processing system, where a
external tool/server initiates some job and this system needs to:

- process the initial job and gather information regarding the job
- decompose the initial job to smaller pieces
- send those smaller pieces to one dedicated entity in the system which
  processes these pieces.  These job processors shall be created
  on-the-fly.

Requirements
============

**R001 Job Queues Purgeable**
    The administrator shall be enabled to use the RabbitMQ admin gui to
    purge the job queues.  The system shall handle this gracefully.

Boundary Conditions
===================

We have some conditions for which we need to design as follows:

- The initial job needs to be decomposed into smaller pieces
- The set of jobs created from the decomposition need to be handled
  by a separate entity as the job processing itself is very instable

Architecture
============

We'll have the following components:

**master**
    Responsible to listen for new job announcements.  Starts a new *slave*
    for each announcement which will handle the jobs.

**slave**
    The slave which will handle the jobs coming in.  The slave will listen
    on its own queue.

Links
=====

**kombu docs**
    http://ask.github.com/kombu/index.html

**kombu nexiles fork**
    git@github.com:nexiles/kombu.git

..  vim: set ft=rst tw=75 nocin nosi ai sw=4 ts=4 expandtab:
