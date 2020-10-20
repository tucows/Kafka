#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    t/lib
    ../lib
);

use Test::More;

BEGIN {
    eval 'use Test::Exception';     ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval 'use Test::TCP';           ## no critic
    plan skip_all => "because Test::TCP required for testing" if $@;
}

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

plan 'no_plan';

use Test::MockModule;
use IO::Socket::INET;
use IO::Socket::SSL;
use Net::EmptyPort qw(
    empty_port
);
use POSIX ':signal_h';
use Socket qw(
    AF_INET
    AF_INET6
    PF_INET
    PF_INET6
    inet_aton
    inet_ntop
);
use Sub::Install;
use Sys::SigAction qw(
    set_sig_handler
);
use Time::HiRes qw();

use Kafka qw(
    $IP_V4
    $IP_V6
    $KAFKA_SERVER_PORT
    $REQUEST_TIMEOUT
);
use Kafka::IO;
use Kafka::IO::SSL;
use Kafka::TestInternals qw(
    @not_posint
    @not_posnumber
    @not_string
);

# See Kafka::IO
use constant DEBUG  => 0;
#use constant DEBUG  => 1;
#use constant DEBUG  => 2;

Kafka::IO::SSL->debug_level( DEBUG ) if DEBUG;

STDOUT->autoflush;

my ( $server, $port, $io, $sig_handler, $marker_signal_handling, $original, $timer, $timeout, $sent, $resp, $test_message, $inet_aton, $hostname );

$inet_aton = inet_aton( '127.0.0.1' );  # localhost
$hostname = gethostbyaddr( $inet_aton, AF_INET );

my $io_socket_ssl_mock = Test::MockModule->new('IO::Socket::SSL');
# ignore ssl as we test Kafka::IO::SSL module only
$io_socket_ssl_mock->mock('start_SSL', sub { 1 });

my $io_socket_inet_mock = Test::MockModule->new('IO::Socket::INET');
# define IO::Socket::SSL methods on IO::Socket::INET module
my %io_socket_inet_mock_args = ( 
    # Initializing the object.
    read => sub {
        my $self = shift;
        my $from = $self->recv($_[0], $_[1]);
    },
    print => sub { my ($self, @args) = @_; $self->send(@args); }
);

$io_socket_inet_mock->mock($_ => $io_socket_inet_mock_args{$_}) for keys %io_socket_inet_mock_args;

my $server_code = sub {
    my ( $port ) = @_;

    my $sock = IO::Socket::INET->new(
        LocalPort   => $port,
        LocalAddr   => $hostname,
        Proto       => 'tcp',
        Listen      => 5,
        Type        => SOCK_STREAM,
        ReuseAddr   => 1,
    ) or die "Cannot open server socket $hostname:$port : $!";

    $SIG{TERM} = sub { exit };

    while ( my $remote = $sock->accept ) {
        while ( my $line = <$remote> ) {
            print { $remote } $line;
        }
    }
};

sub debug_msg {
    my ( $message ) = @_;

    return if Kafka::IO->debug_level != 2;

    diag '[ time = ', Time::HiRes::time(), ' ] ', $message;
}

my $server_port = empty_port( $KAFKA_SERVER_PORT );
$server = Test::TCP->new(
    code    => $server_code,
    port    => $server_port,
);
$port = $server->port;
ok $port, "server port = $port";
wait_port( $port );

$test_message = "Test message\n";

#NOTE: Kafka::IO->new uses alarm clock internally
# -- ALRM handler

debug_msg( 'ALRM handler verification' );

# cancel the previous timer
alarm 0;

$sig_handler = set_sig_handler( SIGALRM ,sub {
        ++$marker_signal_handling;
        debug_msg( 'SIGALRM: signal handler triggered' );
    }
);
ok( !defined( $marker_signal_handling ), 'marker signal handling not defined' );

throws_ok {
    debug_msg( "ALRM handler: host => 'something bad'" );
    $io = Kafka::IO::SSL->new(
        host    => 'something bad',
        port    => $port,
        timeout => $REQUEST_TIMEOUT,
    );
} 'Kafka::Exception::IO', 'error thrown';

debug_msg( "ALRM handler: host => $hostname" );
eval {
    $io = Kafka::IO::SSL->new(
        host    => $hostname,
        port    => $port,
        timeout => $REQUEST_TIMEOUT,
    );
};
SKIP: {
    isa_ok( $io, 'Kafka::IO::SSL' );

    ok( !defined( $marker_signal_handling ), 'marker signal handling not defined' );
    # signal handler triggered
    kill ALRM => $$;
    is $marker_signal_handling, 1, 'the signal handler to be reset to the previous value';

    #-- ALRM timer

    # Kafka::IO->new is badly ended before 'timer' and before 'timeout'

    # cancel the previous timer
    alarm 0;

    $SIG{ALRM} = sub {
        ++$marker_signal_handling;
        debug_msg( 'SIGALRM: signal handler triggered' );
    };
    $timer      = 10;
    $timeout    = $timer;

    debug_msg( "Kafka::IO::SSL->new is badly ended before 'timer' and before 'timeout'" );
    debug_msg( "timer = $timer, timeout = $timeout, host => 'something bad'" );
    $marker_signal_handling = 0;
    eval {
        alarm $timer;
        eval {
            $io = Kafka::IO::SSL->new(
                host    => 'something bad',
                port    => $port,
                timeout => $timeout,
            );
        };
        alarm 0;
    };
    ok !$marker_signal_handling, 'signal handler is not triggered';

    # Kafka::IO->new is correctly ended before 'timer' and before 'timeout'

    # cancel the previous timer
    alarm 0;

    $io = Kafka::IO::SSL->new(
        host    => $hostname,
        port    => $port,
        timeout => $REQUEST_TIMEOUT,
    );

    #-- close

    ok $io->{socket}, 'socket defined';
    $io->close;
    ok !$io->{socket}, 'socket not defined';


    #-- send

    $io = Kafka::IO::SSL->new(
        host    => $hostname,
        port    => $port,
        timeout => $REQUEST_TIMEOUT,
    );

    lives_ok { $sent = $io->send( $test_message ); } 'expecting to live';
    is $sent, length( $test_message ), 'sent '.length( $test_message ).' bytes';

    #-- receive

    lives_ok { $resp = $io->receive( length( $test_message ) ); } 'expecting to live';
    is( $$resp, $test_message, 'receive OK' );

    #-- send undef message 
    throws_ok { $sent = $io->send(undef); } 'Kafka::Exception::IO', 'error thrown';
    #-- recieve zero length
    throws_ok { $resp = $io->receive(0); } 'Kafka::Exception::IO', 'error thrown';

    #-- recieve closed socket
    $io->close();
    throws_ok { $sent = $io->send($test_message); } 'Kafka::Exception::IO', 'error thrown';
    throws_ok { $resp = $io->receive(length( $test_message )); } 'Kafka::Exception::IO', 'error thrown';

    


    #throws_ok { $sent = $io->send( $test_message ); } 'Kafka::Exception::IO', 'error thrown';
}

undef $server;

