package DBIx::MySQL::Replication::Slave;

use Moose;

=head1 NAME

DBIx::MySQL::Replication::Slave - Stop, start and monitor your slaves.

=head1 SYNOPSIS

DBIx::MySQL::Replication::Slave issues a "SHOW SLAVE STATUS" query and returns
the results to you as a HASHREF. It also includes the slave_ok() method, which
is a handy shortcut to see whether your slave server requires any special
attention. It doesn't do anything you can't already do for yourself, but it
makes it just a little bit quicker to check on the health of your slaves.

    use DBIx::MySQL::Replication::Slave;
    
    my $slave = DBIx::MySQL::Replication::Slave->new( dbh => $dbh );
    my $status = $slave->status;
    print "seconds behind: " . $status->{'seconds_behind_master'};

=head1 CONSTRUCTOR AND STARTUP

=head2 new( dbh => $dbh )

Creates and returns a new DBIx::MySQL::Replication::Slave object.

    my $slave = DBIx::MySQL::Replication::Slave->new( dbh => $dbh );
    
=over 4

=item * C<< dbh => $dbh >>

A valid database handle to your slave server is required.  You'll need to pass
it to the constructer:

    my $slave = DBIx::MySQL::Replication::Slave->new( dbh => $dbh );

=item * C<< lc => 0|1 >>

By default, the status variables returned by MySQL are converted to lower case.
This is for readability. You may turn this off if you wish, by explicitly
turning it off when you create the object:

    my $slave = DBIx::MySQL::Replication::Slave->new( dbh => $dbh, lc => 0 );

=item * C<< max_seconds_behind_master => $seconds >>

By default this is set to a very generous number (86400 seconds). Set this
value if you'd like to take a shorter amount of time into account when
checking on your health. This is strongly recommended:

    # Anything longer than 30 seconds is not acceptable
    my $slave = DBIx::MySQL::Replication::Slave->new(
        dbh => $dbh,
        seconds_behind_master => 30
    );

If you think it's cleaner, you can also set this value *after* object creation.

    $slave->max_seconds_behind_master(30); 

=back

=head1 SUBROUTINES/METHODS

=head2 status

Returns a HASHREF of the MySQL slave status variables.  These vars will, by
default, be converted to lower case, unless you have turned this off when you
construct the object.  See the lc option to new() for more info.

=head2 start

Issues a "START SLAVE" query and returns DBI's raw return value directly to
you.

=head2 stop

Issues a "STOP SLAVE" query and returns DBI's raw return value directly to
you.

=head2 slave_ok

This method returns true if slave_io_running and slave_sql_running are both
equal to 'Yes' AND if seconds_behind_master is <= max_seconds_behind master.

=head2 is_running

Returns true if both slave_io_running and slave_sql_running are set to 'Yes'

=head2 is_stopped

Returns true if both slave_io_running and slave_sql_running are set to 'No'.
If only one of these values returns 'Yes', it's probably fair to say that the
slave is in some transitional state. Neither stopped nor running may be an
accurate description in this case.

=cut

has 'dbh' => (
    isa      => 'DBI::db',
    is       => 'rw',
    required => 1,
);

has 'lc' => (
    default  => 1,
    is       => 'rw',
    isa      => 'Str',
    required => 0,
);

has 'max_seconds_behind_master' => (
    default  => 86400,
    is       => 'rw',
    isa      => 'Str',
    required => 0,
);

has '_lc_status' => (
    is  => 'rw',
    isa => 'HashRef',
);

has '_status' => (
    is         => 'rw',
    isa        => 'HashRef',
    lazy_build => 1,
);

sub status {
    my $self = shift;
    return $self->_status if !$self->lc;
    return $self->_get_lc_status;
}

sub slave_ok {

    my $self   = shift;
    my $status = $self->_get_lc_status;

    if (   $status->{slave_io_running} eq 'Yes'
        && $status->{slave_sql_running} eq 'Yes'
        && $status->{seconds_behind_master}
        <= $self->max_seconds_behind_master )
    {
        return 1;
    }

    return 0;

}

sub stop {
    
    my $self = shift;
    return $self->dbh->do("STOP SLAVE");
    
}

sub is_stopped {
    
    my $self = shift;
    $self->refresh_status;

    my $status = $self->_get_lc_status;

    if (   $status->{slave_io_running} eq 'No'
        && $status->{slave_sql_running} eq 'No' )
    {
        return 1;
    }   
    
    return 0;
}

sub start {
    
    my $self = shift;    
    return $self->dbh->do("START SLAVE");
    
}

sub is_running {
    
    my $self = shift;
    
    # allow some time to connect, if need be
    foreach (1..10) {
        $self->refresh_status;
        print "slave io: " . $self->status->{slave_io_state} . "\n";
        if ( $self->status->{slave_io_state} ne 'Connecting to master' ) {
            last;
        }
        else {
            sleep 1;
        }
    }

    my $status = $self->_get_lc_status;

    if (   $status->{slave_io_running} eq 'Yes'
        && $status->{slave_sql_running} eq 'Yes' )
    {
        return 1;
    }   
    
    return 0;
}

sub refresh_status {
    
    my $self = shift;
    $self->status( $self->_build__status );
    return;
    
}

sub _build__status {

    my $self   = shift;
    my $status = $self->dbh->selectrow_hashref( "SHOW SLAVE STATUS" );

    my $lc = {};
    foreach my $col ( keys %{$status} ) {
        $lc->{ lc $col } = $status->{$col};
    }

    $self->_lc_status( $lc );
    return $status;

}

sub _get_lc_status {

    my $self = shift;
    $self->_status;
    return $self->_lc_status;

}

1;
