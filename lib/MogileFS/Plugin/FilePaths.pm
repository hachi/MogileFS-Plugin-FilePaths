# FilePaths plugin for MogileFS, by xb95 and hachi

#
# This plugin enables full pathing support within MogileFS, for creating files,
# listing files in a directory, deleting files, etc.
#
# Supports most functionality you'd expect.

package MogileFS::Plugin::FilePaths;

use strict;
use warnings;

our $VERSION = '0.03';
$VERSION = eval $VERSION;

use MogileFS::FID;
use MogileFS::Worker::Query;
use MogileFS::Plugin::MetaData;
use MogileFS::Plugin::FilePaths::Node;

sub _parse_path {
    my $fullpath = shift;
    return unless defined($fullpath) and length($fullpath);
    my ($path, $file) = $fullpath =~
        m!^(/(?:[\w\-\.]+/)*)([\w\-\.]+)$!;
    return ($path, $file);
}

# called when this plugin is loaded, this sub must return a true value in order for
# MogileFS to consider the plugin to have loaded successfully.  if you return a
# non-true value, you MUST NOT install any handlers or other changes to the system.
# if you install something here, you MUST un-install it in the unload sub.
sub load {

    # we want to remove the key being passed to create_open, as it is going to contain
    # only a path, and we want to ignore that for now
    MogileFS::register_global_hook( 'cmd_create_open', sub {
        my $args = shift;
        return 1 unless _check_dmid($args->{dmid});

        my $fullpath = delete $args->{key};
        my ($path, $filename) = _parse_path($fullpath);
        die "Filename is not a valid absolute path."
            unless defined($path) && length($path) && defined($filename) && length($filename);
        return 1;
    });

    # when people try to create new files, we need to intercept it and rewrite the
    # request a bit in order to do the right footwork to support paths.
    MogileFS::register_global_hook( 'cmd_create_close', sub {
        my $args = shift;
        return 1 unless _check_dmid($args->{dmid});

        # the key is the path, so we need to move that into the logical_path argument
        # and then set the key to be something more reasonable
        $args->{logical_path} = $args->{key};
        $args->{key} = "fid:$args->{fid}";
    });

    # called when we know a file has successfully been uploaded to the system, it's
    # a done deal, we don't have to worry about anything else
    MogileFS::register_global_hook( 'file_stored', sub {
        my $args = shift;
        return 1 unless _check_dmid($args->{dmid});

        # we need a path or this plugin is moot
        return 0 unless $args->{logical_path};

        # ensure we got a valid seeming path and filename
        my ($path, $filename) = _parse_path($args->{logical_path});
        return 0 unless defined($path) && length($path) && defined($filename) && length($filename);

        # great, let's vivify that path and get the node to it
        my $parentnode = vivify_path( $args->{dmid}, $path );
        return 0 unless defined $parentnode;

        # find/create a node to store this file at, track the old FID in order
        # to delete it after updating the node
        my $sto = Mgd::get_store();
        my $node = $sto->plugin_filepaths_get_node_by_parent($args->{dmid}, $parentnode->id, $filename);
        my $oldfid = $node ? $node->fid : undef;
        if($node) {
            $sto->plugin_filepaths_update_node($node->id, {'fid' => $args->{fid}});
        } else {
            my $nodeid = $sto->plugin_filepaths_add_node(
                'dmid'         => $args->{dmid},
                'parentnodeid' => $parentnode->id,
                'nodename'     => $filename,
                'fid'          => $args->{fid},
            );
            $node = MogileFS::Plugin::FilePaths::Node->new($nodeid);
        }
        return 0 unless $node;

        # delete the old FID now that the new FID has been stored
        if ($oldfid) {
            $oldfid->delete;
        }

        # store metadata
        if (my $keys = $args->{"plugin.meta.keys"}) {
            my %metadata;
            for (my $i = 0; $i < $keys; $i++) {
                my $key = $args->{"plugin.meta.key$i"};
                my $value = $args->{"plugin.meta.value$i"};
                $metadata{$key} = $value;
            }

            MogileFS::Plugin::MetaData::set_metadata($args->{fid}, \%metadata);
        }

        # we're successful, let's keep the file
        return 1;
    });

    # and now magic conversions that make the rest of the MogileFS commands work
    # without having to understand how the path system works
    MogileFS::register_global_hook( 'cmd_get_paths', \&_path_to_key );
    MogileFS::register_global_hook( 'cmd_file_info', \&_path_to_key );
    MogileFS::register_global_hook( 'cmd_file_debug', \&_path_to_key );
    MogileFS::register_global_hook( 'cmd_delete', sub {
        my $args = shift;
        return 1 unless _check_dmid($args->{dmid});

        # ensure we got a valid seeming path and filename
        my ($path, $filename) = _parse_path($args->{key});
        return 0 unless defined($path) && length($path) && defined($filename) && length($filename);

        # now try to get the end of the path
        my $parentnodeid = MogileFS::Plugin::FilePaths::load_path( $args->{dmid}, $path );
        return 0 unless defined $parentnodeid;

        # get the fid of the file, bail out if it doesn't have one (directory nodes)
        my $sto = Mgd::get_store();
        my $node = $sto->plugin_filepaths_get_node_by_parent($args->{dmid}, $parentnodeid, $filename);
        return 0 unless $node;
        my $fidid = $node->fidid;
        return 0 unless $fidid;

        # great, delete this file
        $sto->plugin_filepaths_delete_node($node->id);
        # FIXME What should happen if this delete fails?

        # now pretend they asked for it and continue
        $args->{key} = 'fid:' . $fidid;
    });

    MogileFS::register_worker_command( 'filepaths_enable', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        # enable the FilePaths plugin for the specified domain
        my $sto = Mgd::get_store();
        unless ($sto->plugin_filepaths_enable_domain($dmid)) {
            return $self->err_line('unable_to_enable', 'Unable to enable the FilePaths plugin');
        }
        return $self->ok_line;
    });

    MogileFS::register_worker_command( 'filepaths_disable', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        # disable the FilePaths plugin for the specified domain
        my $sto = Mgd::get_store();
        unless ($sto->plugin_filepaths_disable_domain($dmid)) {
            return $self->err_line('unable_to_disable', 'Unable to disable the FilePaths plugin');
        }
        return $self->ok_line;
    });

    # now let's define the extra plugin commands that we allow people to interact with us
    # just like with a regular MogileFS command
    MogileFS::register_worker_command( 'filepaths_list_directory', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        return $self->err_line("plugin_not_active_for_domain")
            unless _check_dmid($dmid);

        # verify arguments - only one expected, make sure it starts with a /
        my $path = $args->{arg1};
        return $self->err_line('bad_params')
            unless $args->{argcount} == 1 && $path && $path =~ /^\//;

        # now find the id of the path
        my $nodeid = load_path( $dmid, $path );
        return $self->err_line('path_not_found', 'Path provided was not found in database')
            unless defined $nodeid;

#       TODO This is wrong, but we should throw an error saying 'not a directory'. Requires refactoring
#            a bit of code to make the 'fid' value available from the last node we fetched.
#        if (get_file_mapping($nodeid)) {
#            return $self->err_line('not_a_directory', 'Path provided is not a directory');
#        }

        # get files in path, return as an array
        my %res;
        my $ct = 0;
        my $sto = Mgd::get_store();
        my @nodes = $sto->plugin_filepaths_get_nodes_by_parent($dmid, $nodeid);

        # get FIDs for all the found nodes
        my %fids = (
            map {($_->id, $_)}
                $sto->plugin_filepaths_load_fids(map {$_->fidid} @nodes)
        );

        # add all nodes to the response
        my $i = 0;
        foreach my $node (@nodes) {
            next if(!$node);

            my $prefix = "file$i";
            $res{$prefix} = $node->nodename;

            # This node is a directory
            if ($node->is_directory) {
                $res{"$prefix.type"} = "D";
            }
            # This file is a regular file
            elsif(my $fid = $fids{$node->fidid} || $node->fid) {
                # skip this node unless the fid exists
                next unless $fid->exists;

                $res{"$prefix.type"} = "F";
                my $length = $fid->length;
                $res{"$prefix.size"} = $length if defined($length);
                my $metadata = MogileFS::Plugin::MetaData::get_metadata($fid->id);
                $res{"$prefix.mtime"} = $metadata->{mtime} if $metadata->{mtime};
            }
            # invalid node, don't include it in the response
            else {
                next;
            }

            $i++;
        }
        $res{'files'} = $i;

        return $self->ok_line( \%res );
    });

    MogileFS::register_worker_command( 'filepaths_rename', sub {
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        return $self->err_line("plugin_not_active_for_domain")
            unless _check_dmid($dmid);

        return $self->err_line("bad_argcount")
            unless $args->{argcount} == 2;

        my ($old_path, $old_name) = _parse_path($args->{arg1});

        return $self->err_line("badly_formed_orig")
            unless defined($old_path) && length($old_path) &&
                   defined($old_name) && length($old_name);

        my ($new_path, $new_name) = _parse_path($args->{arg2});

        return $self->err_line("badly_formed_new")
            unless defined($new_path) && length($new_path) &&
                   defined($new_name) && length($new_name);

        # I'd really like to lock on this operation at this point, but I find the whole idea to be rather
        # sad for the number of locks I would want to hold. Going to think about this instead and hope
        # nobody finds a way to make this race.

        # LOCK rename

        # find the node being renamed
        my $old_parentid = load_path($dmid, $old_path);
        my $sto = Mgd::get_store();
        my $node = $sto->plugin_filepaths_get_node_by_parent($dmid, $old_parentid, $old_name);
        return $self->err_line('path_not_found', 'Path provided was not found in database')
            unless $node;

        # now vivify the destination path and rename the file
        my $new_parent = vivify_path($dmid, $new_path);
        return $self->err_line("rename_failed") unless $new_parent;
        my $rv = $sto->plugin_filepaths_update_node($node->id, {
            'parentnodeid' => $new_parent->id,
            'nodename'     => $new_name,
        });

        # UNLOCK rename

        return $self->err_line("rename_failed") unless $rv;

        return $self->ok_line();
    });

    return 1;
}

# this sub is called at the end or when the module is being unloaded, this needs to
# unregister any registered methods, etc.  you MUST un-install everything that the
# plugin has previously installed.
sub unload {

    # remove our hooks
    MogileFS::unregister_global_hook( 'cmd_create_open' );
    MogileFS::unregister_global_hook( 'cmd_create_close' );
    MogileFS::unregister_global_hook( 'file_stored' );

    return 1;
}

# called when you want to create a path, this will break down the given argument and
# create any elements needed, returning the nodeid of the final node.  returns undef
# on error, else, 0-N is valid.
sub vivify_path {
    my ($dmid, $path) = @_;
    return undef unless $dmid && $path;
    return _traverse_path($dmid, $path, 1);
}

# called to load the nodeid of the final element in a path, which is useful for finding
# out if a path exists.  does NOT automatically create path elements that don't exist.
sub load_path {
    my ($dmid, $path) = @_;
    return undef unless $dmid && $path;
    my $node = _traverse_path($dmid, $path, 0);
    return $node ? $node->id : undef;
}

# does the internal work of traversing a path
sub _traverse_path {
    my ($dmid, $path, $vivify) = @_;
    return undef unless $dmid && $path;

    # start with the root path node
    my $node = MogileFS::Plugin::FilePaths::Node->new(0);

    # recurse the specified path
    foreach my $part (grep { $_ } split /\//, $path) {
        # look for the current path part
        $node = _find_node($dmid, $node->id, $part, $vivify);
        return undef unless $node;
    }

    # we're done, so return the most recent node
    return $node;
}

# checks to see if a node exists, and if not, creates it if $vivify is set
sub _find_node {
    my ($dmid, $parentnodeid, $name, $vivify) = @_;
    return undef unless $dmid && defined $parentnodeid && $name;

    my $sto = Mgd::get_store();
    my $node = $sto->plugin_filepaths_get_node_by_parent($dmid, $parentnodeid, $name);
    return $node if $node;

    if ($vivify) {
        my $nodeid = $sto->plugin_filepaths_add_node(
            'dmid'         => $dmid,
            'parentnodeid' => $parentnodeid,
            'nodename'     => $name,
        );

        return MogileFS::Plugin::FilePaths::Node->new($nodeid) if $nodeid && $nodeid > 0;
    }

    return undef;
}

# generic sub that converts a file path to a key name that
# MogileFS will understand
sub _path_to_key {
    my $args = shift;

    my $dmid = $args->{dmid};
    return 1 unless _check_dmid($dmid);

    # ensure we got a valid seeming path and filename
    my ($path, $filename) = _parse_path($args->{key});
    return 0 unless $path && $filename;

    # now try to get the end of the path
    my $parentnodeid = MogileFS::Plugin::FilePaths::load_path( $dmid, $path );
    return 0 unless defined $parentnodeid;

    # great, find this file
    my $sto = Mgd::get_store();
    my $node = $sto->plugin_filepaths_get_node_by_parent($dmid, $parentnodeid, $filename);
    my $fidid = $node->fidid;
    return 0 unless $fidid;

    # now pretend they asked for it and continue
    $args->{key} = 'fid:' . $fidid;
    return 1;
}

my $active_dmids = {};
my $last_dmid_check = 0;

sub _check_dmid {
    my $dmid = shift;

    return unless defined $dmid;

    my $time = time();
    if ($time >= $last_dmid_check + 15) {
        $last_dmid_check = $time;

        my $sto = Mgd::get_store();
        my $dmids = $sto->plugin_filepaths_get_active_dmids();

        if (defined $dmids) {
            $active_dmids = {};
            foreach (@$dmids) {
                $active_dmids->{$_} = 1;
            }
        } else {
            warn "Unable to load active domains list for filepaths plugin, using old list";
        }
    }

    return $active_dmids->{$dmid};
}

package MogileFS::Store;

use MogileFS::Store;

use strict;
use warnings;

sub TABLE_plugin_filepaths_paths {
    "CREATE TABLE plugin_filepaths_paths (
        nodeid BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        dmid SMALLINT UNSIGNED NOT NULL,
        parentnodeid BIGINT UNSIGNED NOT NULL,
        nodename VARCHAR(255) BINARY NOT NULL,
        fid BIGINT UNSIGNED,
        PRIMARY KEY (nodeid),
        UNIQUE KEY (dmid, parentnodeid, nodename)
)"
}

sub TABLE_plugin_filepaths_domains {
    "CREATE TABLE plugin_filepaths_domains (
        dmid SMALLINT UNSIGNED NOT NULL,
        PRIMARY KEY (dmid)
)"
}

# enable the filepaths plugin on the specified domain
sub plugin_filepaths_enable_domain {
    my $self = shift;
    my ($dmid) = @_;
    my $dbh = $self->dbh;
    $self->retry_on_deadlock(sub {
        $dbh->do($self->ignore_replace . 'INTO plugin_filepaths_domains (dmid) VALUES (?)', undef, $dmid);
    });
    return undef if($dbh->err);
    return 1;
}

# disable the filepaths plugin on the specified domain
sub plugin_filepaths_disable_domain {
    my $self = shift;
    my ($dmid) = @_;
    my $dbh = $self->dbh;
    $self->retry_on_deadlock(sub {
        $dbh->do('DELETE FROM plugin_filepaths_domains WHERE dmid = ?', undef, $dmid);
    });
    return undef if($dbh->err);
    return 1;
}

# retrieves an arrayref of dmids the filepaths plugin is active for
sub plugin_filepaths_get_active_dmids {
    my $self = shift;
    my $dbh = $self->dbh;
    my $dmids = $dbh->selectcol_arrayref('SELECT dmid FROM plugin_filepaths_domains');
    return undef if $dbh->err;
    return $dmids;
}

# add a new node to the database
sub plugin_filepaths_add_node {
    my $self = shift;
    my %arg  = $self->_valid_params([qw(dmid parentnodeid nodename fid)], @_);

    return $self->retry_on_deadlock(sub {
        my $dbh = $self->dbh;
        $dbh->do('INSERT INTO plugin_filepaths_paths (dmid, parentnodeid, nodename, fid) '.
                 'VALUES (?,?,?,?) ', undef,
                 @arg{'dmid', 'parentnodeid', 'nodename', 'fid'});
        return $dbh->last_insert_id(undef, undef, 'plugin_filepaths_paths', 'nodeid')
    });
}

# update the specified node in the database
sub plugin_filepaths_update_node {
    my $self = shift;
    my ($nodeid, $to_update) = @_;
    my @keys = keys %$to_update;
    return 1 unless @keys;
    my $dbh = $self->dbh;
    $self->retry_on_deadlock(sub {
        $dbh->do('UPDATE plugin_filepaths_paths SET ' . join('=?, ', @keys) .
                 '=? WHERE nodeid = ?', undef, @$to_update{@keys}, $nodeid);
    });
    return undef if $dbh->err;
    return 1;
}

# delete the specified node from the database
sub plugin_filepaths_delete_node {
    my $self = shift;
    my ($nodeid) = @_;
    my $dbh = $self->dbh;
    $self->retry_on_deadlock(sub {
        $dbh->do('DELETE FROM plugin_filepaths_paths WHERE nodeid = ?', undef, $nodeid);
    });
    return undef if $dbh->err;
    return 1;
}

#returns a hash ref containing
sub plugin_filepaths_node_row_from_nodeid {
    my ($self, $nodeid) = @_;
    return $self->dbh->selectrow_hashref("SELECT nodeid, dmid, parentnodeid, nodename, fid ".
                                         "FROM plugin_filepaths_paths WHERE nodeid=?",
                                         undef, $nodeid);
}

# return the node for the specified node
sub plugin_filepaths_get_node_by_parent {
    my $self = shift;
    my ($dmid, $parentnodeid, $nodename) = @_;
    my $dbh = $self->dbh;
    my $row = $dbh->selectrow_hashref('SELECT nodeid, dmid, parentnodeid, nodename, fid ' .
                                      'FROM plugin_filepaths_paths ' .
                                      'WHERE dmid = ? AND parentnodeid = ? AND nodename = ?',
                                      undef, $dmid, $parentnodeid, $nodename);
    return undef if !$row || $dbh->err;
    return MogileFS::Plugin::FilePaths::Node->new_from_db_row($row);
}

# get all the nodes that are child nodes of the specified parent node
sub plugin_filepaths_get_nodes_by_parent {
    my $self = shift;
    my ($dmid, $parentnodeid) = @_;
    my $dbh = $self->dbh;
    my $sth = $dbh->prepare('SELECT nodeid, dmid, parentnodeid, nodename, fid ' .
                            'FROM plugin_filepaths_paths ' .
                            'WHERE dmid = ? AND parentnodeid = ?');
    $sth->execute($dmid, $parentnodeid);

    my @nodes;
    while (my $row = $sth->fetchrow_hashref()) {
        push @nodes, MogileFS::Plugin::FilePaths::Node->new_from_db_row($row);
    }

    return @nodes;
}

# load the specified fids from the database
sub plugin_filepaths_load_fids {
    my $self = shift;
    my @fids = @_;
    return if(!@fids);

    my @ret;
    my $dbh = $self->dbh;
    my $sth = $dbh->prepare("SELECT fid, dmid, dkey, length, classid, devcount ".
                            "FROM   file ".
                            "WHERE  fid IN (". join(',', (('?') x scalar @fids)) . ")");
    $sth->execute(@fids);
    while (my $row = $sth->fetchrow_hashref) {
        push @ret, MogileFS::FID->new_from_db_row($row);
    }
    return @ret;
}

__PACKAGE__->add_extra_tables("plugin_filepaths_paths", "plugin_filepaths_domains");

1;
