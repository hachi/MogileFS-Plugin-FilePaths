package MogileFS::Plugin::FilePaths::Node;

use strict;
use warnings;
use Carp qw(croak);

sub new {
    my ($class, $nodeid) = @_;
    croak("Invalid nodeid") unless defined($nodeid);
    return bless {
        nodeid       => $nodeid,
        nodename     => undef,
        dmid         => undef,
        parentnodeid => undef,
        fid          => undef,
        _loaded      => 0,
    }, $class;
}

# mutates/blesses given row.
sub new_from_db_row {
    my ($class, $row) = @_;
    die "Missing 'nodeid' column" if(!$row->{nodeid});
    $row->{_loaded} = 1;
    return bless $row, $class;
}

sub id { $_[0]{nodeid} }

sub fidid {
    my $self = shift;
    $self->_load;
    return $self->{fid};
}

# force loading, or die.
sub _load {
    return 1 if $_[0]{_loaded};
    my $self = shift;
    croak('NODE#'.$_[0]->id.' doesn\'t exist') unless $self->_tryload;
}

# return 1 if loaded, or 0 if not exist
sub _tryload {
    return 1 if $_[0]{_loaded};
    my $self = shift;

    # special case for root directory node
    if($self->{nodeid} == 0) {
        $self->{_loaded} = 1;
        return 1;
    }

    my $row = Mgd::get_store()->plugin_filepaths_node_row_from_nodeid($self->{nodeid})
        or return 0;
    $self->{$_} = $row->{$_} foreach qw(nodename dmid parentnodeid fid);
    $self->{_loaded} = 1;
    return 1;
}

sub fid {
    my $fidid = $_[0]->fidid;
    return MogileFS::FID->new($fidid) if($fidid);
    return undef;
}

1;
