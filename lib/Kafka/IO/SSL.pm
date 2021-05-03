package Kafka::IO::SSL;

=head1 NAME

Kafka::IO::SSL - SSL interface to nonblocking network communication with the Apache Kafka server with Coro.
This module implements the same interface that usual Kafka::IO module

=head1 VERSION

Read documentation for C<Kafka::IO> version 1.09 .

=cut



use 5.010;
use strict;
use warnings;



our $DEBUG = 0;

our $VERSION = 'v1.09';

use Carp;
use Config;
use Const::Fast;
use Fcntl;
use Params::Util qw(
    _STRING
);
use Scalar::Util qw(
    dualvar
);
use Try::Tiny;

use Kafka qw(
    $ERROR_CANNOT_BIND
    $ERROR_CANNOT_RECV
    $ERROR_CANNOT_SEND
    $ERROR_MISMATCH_ARGUMENT
    $ERROR_NO_CONNECTION
    $KAFKA_SERVER_PORT
    $REQUEST_TIMEOUT
);
use Kafka::Exceptions;
use Kafka::Internals qw(
    $MAX_SOCKET_REQUEST_BYTES
    debug_level
    format_message
);
use IO::Socket::SSL;
use IO::Socket;
use Data::Dumper;
use Errno qw(
    EAGAIN
    ECONNRESET
    EINTR
    EWOULDBLOCK
    ETIMEDOUT
);
=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    use Kafka::IO::Async;

    my $io;
    try {
        $io = Kafka::IO::SSL->new( host => 'localhost' );
    } catch {
        my $error = $_;
        if ( blessed( $error ) && $error->isa( 'Kafka::Exception' ) ) {
            warn 'Error: (', $error->code, ') ',  $error->message, "\n";
            exit;
        } else {
            die $error;
        }
    };

    # Closes and cleans up
    $io->close;
    undef $io;

=head1 DESCRIPTION

This module is private and should not be used directly.

In order to achieve better performance, methods of this module do not
perform arguments validation.

The main features of the C<Kafka::IO::SSL> class are:

=over 3

=item *

Provides an object oriented API for communication with Kafka.

=item *

This class allows you to create Kafka 0.9+ clients.

=back

=cut

# Hard limit of IO operation retry attempts, to prevent high CPU usage in IO retry loop
const my $MAX_RETRIES => 30;

our $_hdr;

#-- constructor ----------------------------------------------------------------

=head2 CONSTRUCTOR

=head3 C<new>

Establishes secure TCP connection to given host and port, creates and returns C<Kafka::IO::SSL> IO object.

C<new()> takes arguments in key-value pairs. The following arguments are currently recognized:

=over 3

=item C<host =E<gt> $host>

C<$host> is Kafka host to connect to. It can be a host name or an IP-address in
IPv4 or IPv6 form (for example '127.0.0.1', '0:0:0:0:0:0:0:1' or '::1').

=item C<port =E<gt> $port>

Optional, default = C<$KAFKA_SERVER_PORT>.

C<$port> is integer attribute denoting the port number of to access Apache Kafka.

C<$KAFKA_SERVER_PORT> is the default Apache Kafka server port that can be imported
from the L<Kafka|Kafka> module.

=item C<ssl_cert_file =E<gt> $ssl_cert_file>

C<$ssl_cert_file> file path with client certificates, which should be verified by the server.

Supported file formats are PEM, DER and PKCS#12, where PEM and PKCS#12 can contain the certificate and
the chain to use, while DER can only contain a single certificate. If a key was already given
within the PKCS#12 file specified by SSL_cert_file it will ignore any SSL_key or SSL_key_file.
If no SSL_key or SSL_key_file was given it will try to use the PEM file given with SSL_cert_file again,
maybe it contains the key too. For more information see https://metacpan.org/pod/IO::Socket::SSL 

=item C<ssl_key_file =E<gt> $ssl_key_file>

C<$ssl_key_file> For each certificate a key is need, which can either be given as a file with SSL_key_file

=item C<ssl_ca_file =E<gt> $ssl_ca_file>

C<$ssl_ca_file> Usually you want to verify that the peer certificate has been signed by a trusted certificate authority.
In this case you should use this option to specify the file (SSL_ca_file) or directory (SSL_ca_path)
containing the certificate(s) of the trusted certificate authorities.

=item C<ssl_verify_mode =E<gt> $ssl_verify_mode>

C<$VERIFY_MODE> is the default verify mode (SSL_VERIFY_PEER)

C<$ssl_verify_mode> option to set the verification mode for the peer certificate. 

=item C<timeout =E<gt> $timeout>

C<$REQUEST_TIMEOUT> is the default timeout that can be imported from the L<Kafka|Kafka> module.

Special behavior when C<timeout> is set to C<undef>:

=back

=over 3

=item *

Default C<$REQUEST_TIMEOUT> is used for the rest of IO operations.

=back

=cut
sub new {
    my ( $class, %p ) = @_;

    my $self = bless {
        host        => '',
        timeout     => $REQUEST_TIMEOUT,
        port        => $KAFKA_SERVER_PORT,
        ip_version  => undef,
        ssl_cert_file => undef,
        ssl_key_file => undef,
        ssl_ca_file => undef,
        ssl_cert => undef,
        ssl_key => undef,
        ssl_ca => undef,
        ssl_verify_mode => SSL_VERIFY_PEER
    }, $class;
    exists $p{$_} and $self->{$_} = $p{$_} foreach keys %$self;

    # we trust it: make it untainted
    ( $self->{host} ) = $self->{host} =~ /\A(.+)\z/;
    ( $self->{port} ) = $self->{port} =~ /\A(.+)\z/;

    $self->{socket} = undef;
    my $error;
    try {
        $self->_connect();
    } catch {
        $error = $_;
    };

    $self->_error( $ERROR_CANNOT_BIND, format_message("Kafka::IO::SSL(%s:%s)->new: %s", $self->{host}, $self->{port}, $error ) )
        if defined $error;

    return $self;
}

#-- public attributes ----------------------------------------------------------

=head2 METHODS

The following methods are provided by C<Kafka::IO::Async> class:

=cut

=head3 C<< send( $message <, $timeout> ) >>

Sends a C<$message> to Kafka.

The argument must be a bytes string.

Use optional C<$timeout> argument to override default timeout for this request only.

Returns the number of characters sent.

=cut
sub send {
    my ( $self, $message, $timeout ) = @_;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '->send' )
        unless defined( _STRING( $message ) )
    ;
    my $length = length( $message );
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '->send' )
        unless $length <= $MAX_SOCKET_REQUEST_BYTES
    ;
    $timeout = $self->{timeout} // $REQUEST_TIMEOUT unless defined $timeout;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '->send' )
        unless $timeout > 0
    ;

    my $socket = $self->{socket};
    $self->_error( $ERROR_NO_CONNECTION, 'Attempt to work with a closed socket' ) if !$socket;

    my $started = Time::HiRes::time();
    my $until = $started + $timeout;

    my $sent = 0;
    my $errno;
    my $retries = 0; 
    my $interrupts = 0; 
    ATTEMPT: while ( $sent < $length && $retries++ < $MAX_RETRIES ) {
        my $remaining_time = $until - Time::HiRes::time();
        last ATTEMPT if $remaining_time <= 0; # timeout expired

        undef $!;
        my $wrote = $socket->print($message);
        $errno = $!;

        if( defined $wrote && $wrote > 0 ) {
            $sent += $wrote;
            if ( $sent < $length ) {
                # remove written data from message
                $message = substr( $message, $wrote );
            }
        }

        if( $errno ) {
            if( $errno == EINTR ) {
                undef $errno;
                --$retries; # this attempt does not count
                ++$interrupts;
                next ATTEMPT;
            } elsif (
                       $errno != EAGAIN
                    && $errno != EWOULDBLOCK
                    ## on freebsd, if we got ECONNRESET, it's a timeout from the other side
                    && !( $errno == ECONNRESET && $^O eq 'freebsd' )
                ) {
                $self->close;
                last ATTEMPT;
            }
        }

        last ATTEMPT unless defined $wrote;
    }

    unless (defined( $sent ) && $sent == $length ){
        $self->_error(
            $ERROR_CANNOT_SEND,
            format_message( "Kafka::IO(%s)->send: ERROR='%s' (length=%s, sent=%s, timeout=%s, secs=%.6f)",
                $self->{host},
                ( $errno // '<none>' ) . '',
                $length,
                $sent,
                $timeout,
                Time::HiRes::time() - $started,
            )
        );
    }

    return $sent;
}

=head3 C<< receive( $length <, $timeout> ) >>

Receives a message up to C<$length> size from Kafka.

C<$length> argument must be a positive number.

Use optional C<$timeout> argument to override default timeout for this call only.

Returns a reference to the received message.

=cut
sub receive {
    my ( $self, $length, $timeout ) = @_;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '->receive' )
        unless $length > 0
    ;
    $timeout = $self->{timeout} // $REQUEST_TIMEOUT unless defined $timeout;
    $self->_error( $ERROR_MISMATCH_ARGUMENT, '->receive' )
        unless $timeout > 0
    ;
    my $socket = $self->{socket};
    $self->_error( $ERROR_NO_CONNECTION, 'Attempt to work with a closed socket' ) if !$socket;

    my $started = Time::HiRes::time();
    my $until = $started + $timeout;

    my $error;
    my $message = '';
    my $len_to_read = $length;
    my $buf = '';
    my $retries = 0;
    while ( $len_to_read > 0 && $retries++ < $MAX_RETRIES ) {
        my $remaining_time = $until - Time::HiRes::time();
        last if $remaining_time <= 0; # timeout expired

        my $from_recv = $socket->read($buf, $length);
        
        if ( defined( $from_recv ) && length( $buf ) ) {
            $message .= $buf;
            $len_to_read = $length - length( $message );
            --$retries; # this attempt was successful, don't count as a retry
        }
        if ( my $remaining_attempts = $MAX_RETRIES - $retries ) {
            $remaining_time = $until - Time::HiRes::time();
            my $micro_seconds = int( $remaining_time * 1e6 / $remaining_attempts );
            if ( $micro_seconds > 0 ) {
                $micro_seconds = 250_000 if $micro_seconds > 250_000; # prevent long sleeps if total remaining time is big
                $self->_debug_msg( format_message( 'sleeping (remaining attempts %d, time %.6f): %d microseconds', $remaining_attempts, $remaining_time, $micro_seconds ) )
                    if $self->debug_level;
                Time::HiRes::usleep( $micro_seconds );
            }
        }
    }

    unless( length( $message ) >= $length )
    {
        $self->_error(
            $ERROR_CANNOT_RECV,
            format_message( "Kafka::IO(%s)->receive: ERROR='Failed to receive data' (length=%s, received=%s, timeout=%s, secs=%.6f)",
                $self->{host},
                $length,
                length( $message ),
                $timeout,
                Time::HiRes::time() - $started,
            ),
        );
    }
    $self->_debug_msg( $message, 'Response from', 'yellow' )
        if $self->debug_level >= 2;


    return \$message;
}

=head3 C<< try_receive( $length <, $timeout> ) >>
Receives a message up to C<$length> size from Kafka.

C<$length> argument must be a positive number.

Use optional C<$timeout> argument to override default timeout for this call only.

Returns a reference to the received message.

=cut

*try_receive = \&receive;


=head3 C<close>

Closes connection to Kafka server.
Returns true if those operations succeed and if no error was reported by any PerlIO layer.

=cut
sub close {
    my ( $self ) = @_;

    my $ret = 1;
    if ( $self->{socket} ) {
        $self->{socket}->shutdown(SHUT_RDWR);
        $self->{socket} = undef;
    }

    return $ret;
}




#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

# You need to have access to Kafka instance and be able to connect through secure TCP.
sub _connect {
    my ( $self ) = @_;

    $self->{socket} = undef;

    my $host    = $self->{host};
    my $port    = $self->{port};
    my $timeout = $self->{timeout};

    my $sock = IO::Socket::INET->new(
        Timeout => $timeout,
        Type => IO::Socket::SOCK_STREAM,
        proto => 'tcp',
        PeerPort => $port,
        PeerHost => $host, 
    ) || die "Can't open socket: $@";

    my $error = $SSL_ERROR unless IO::Socket::SSL->start_SSL(
        $sock,
        SSL_verify_mode => $self->{ssl_verify_mode},
        (($self->{ssl_cert})? (SSL_cert => $self->{ssl_cert}) : (SSL_cert_file => $self->{ssl_cert_file})),
        (($self->{ssl_key})? (SSL_key => $self->{ssl_key}) : (SSL_key_file => $self->{ssl_key_file})),
        (($self->{ssl_ca})? (SSL_ca => $self->{ssl_ca}) : (SSL_ca_file => $self->{ssl_ca_file})),
    );

    $self->_error( $ERROR_NO_CONNECTION, $error ) if $error;
    
    # Set autoflushing.
    $sock->autoflush(1);

    $self->{socket} = $sock;

    return $sock;
}


# Show additional debugging information
sub _debug_msg {
    my ( $self, $message, $header, $colour ) = @_;

    if ( $header ) {
        unless ( $_hdr ) {
            require Data::HexDump::Range;
            $_hdr = Data::HexDump::Range->new(
                FORMAT                          => 'ANSI',  # 'ANSI'|'ASCII'|'HTML'
                COLOR                           => 'bw',    # 'bw' | 'cycle'
                OFFSET_FORMAT                   => 'hex',   # 'hex' | 'dec'
                DATA_WIDTH                      => 16,      # 16 | 20 | ...
                DISPLAY_RANGE_NAME              => 0,
#                MAXIMUM_RANGE_NAME_SIZE         => 16,
                DISPLAY_COLUMN_NAMES            => 1,
                DISPLAY_RULER                   => 1,
                DISPLAY_OFFSET                  => 1,
#                DISPLAY_CUMULATIVE_OFFSET       => 1,
                DISPLAY_ZERO_SIZE_RANGE_WARNING => 0,
                DISPLAY_ZERO_SIZE_RANGE         => 1,
                DISPLAY_RANGE_NAME              => 0,
#                DISPLAY_RANGE_SIZE              => 1,
                DISPLAY_ASCII_DUMP              => 1,
                DISPLAY_HEX_DUMP                => 1,
#                DISPLAY_DEC_DUMP                => 1,
#                COLOR_NAMES                     => {},
                ORIENTATION                     => 'horizontal',
            );
        }

        say STDERR
            "# $header ", $self->{host}, ':', $self->{port}, "\n",
            '# Hex Stream: ', unpack( 'H*', $message ), "\n",
            $_hdr->dump(
                [
                    [ 'data', length( $message ), $colour ],
                ],
                $message
            )
        ;
    } else {
        say STDERR format_message( '[%s] %s', scalar( localtime ), $message );
    }

    return;
}

# Handler for errors
sub _error {
    my $self = shift;
    my %args = throw_args( @_ );
    $self->_debug_msg( format_message( 'throwing SSL IO error %s: %s', $args{code}, $args{message} ) );
    Kafka::Exception::IO->throw( %args );
}



1;

__END__

=head1 DIAGNOSTICS

When error is detected, an exception, represented by object of C<Kafka::Exception::IO> class,
is thrown (see L<Kafka::Exceptions|Kafka::Exceptions>).

L<code|Kafka::Exceptions/code> and a more descriptive L<message|Kafka::Exceptions/message> provide
information about thrown exception. Consult documentation of the L<Kafka::Exceptions|Kafka::Exceptions>
for the list of all available methods.

Authors suggest using of L<Try::Tiny|Try::Tiny>'s C<try> and C<catch> to handle exceptions while
working with L<Kafka|Kafka> package.

Here is the list of possible error messages that C<Kafka::IO::SSL> may produce:

=over 3

=item C<Invalid argument>

Invalid arguments were passed to a method.

=item C<Cannot send>

Message cannot be sent on a C<Kafka::IO::SSL> object socket.

=item C<Cannot receive>

Message cannot be received.

=item C<Cannot bind>

TCP connection cannot be established on given host and port.

=back

=head2 Debug mode

Debug output can be enabled by passing desired level via environment variable
using one of the following ways:

C<PERL_KAFKA_DEBUG=1>     - debug is enabled for the whole L<Kafka|Kafka> package.

C<PERL_KAFKA_DEBUG=IO:1>  - enable debug for C<Kafka::IO::Async> only.

C<Kafka::IO::SSL> supports two debug levels (level 2 includes debug output of 1):

=over 3

=item 1

Additional information about processing events/alarms.

=item 2

Dump of binary messages exchange with Kafka server.

=back

=head1 SEE ALSO

The basic operation of the Kafka package modules:

L<Kafka|Kafka> - constants and messages used by the Kafka package modules.

L<Kafka::Connection|Kafka::Connection> - interface to connect to a Kafka cluster.

L<Kafka::Producer|Kafka::Producer> - interface for producing client.

L<Kafka::Consumer|Kafka::Consumer> - interface for consuming client.

L<Kafka::Message|Kafka::Message> - interface to access Kafka message
properties.

L<Kafka::Int64|Kafka::Int64> - functions to work with 64 bit elements of the
protocol on 32 bit systems.

L<Kafka::Protocol|Kafka::Protocol> - functions to process messages in the
Apache Kafka's Protocol.

L<Kafka::IO::Async|Kafka::IO::Async> - low-level interface for communication with Kafka server.

L<Kafka::Exceptions|Kafka::Exceptions> - module designated to handle Kafka exceptions.

L<Kafka::Internals|Kafka::Internals> - internal constants and functions used
by several package modules.

A wealth of detail about the Apache Kafka and the Kafka Protocol:

Main page at L<http://kafka.apache.org/>

Kafka Protocol at L<https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol>

=head1 SOURCE CODE

Kafka package is hosted on GitHub:
L<https://github.com/TrackingSoft/Kafka>

=head1 AUTHOR

Sergey Gladkov

Please use GitHub project link above to report problems or contact authors.

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Sergiy Zuban

Vlad Marchenko

Damien Krotkine

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2017 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
