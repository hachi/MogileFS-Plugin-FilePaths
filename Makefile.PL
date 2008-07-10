#!/usr/bin/perl

use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                    => 'MogileFS-Plugin-FilePaths',
    VERSION_FROM            => 'lib/MogileFS/Plugin/FilePaths.pm',
    AUTHOR                  => 'Jonathan Steinert <hachi@cpan.org>',
    ABSTRACT                => 'Plugin for mogilefs server representing a UNIX like filesystem on top of mogilefs.',
    PREREQ_PM               => {
        'MogileFS::Plugin::MetaData'    => '0.01',
        'MogileFS::Server'              => '2.08',
    },
);

