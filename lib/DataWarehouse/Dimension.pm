package DataWarehouse::Dimension;

use warnings;
use strict;

use Carp;
use Data::Dumper;
use DBI;

sub new {
    my ( $class, %params ) = @_;

    croak "Error: One of 'dbh' or 'dsn' parameters is required" if !($params{dbh} xor $params{dsn});
    croak "Error: missing dimension name" if !$params{name};

    if ( $params{dsn} ) {
        $params{dbh} = DBI->connect( $params{dsn}, $params{db_user}, $params{db_password} ),;
    }

    bless {%params}, $class;
}

sub column_names {
    my ($self) = @_;

    my $table = $self->{name};
    warn $table;
    my $columns = $self->{dbh}->column_info( undef, undef, $table, '%' )->fetchall_arrayref();

    my @column_names = map { $_->[3] } @{$columns};
    return wantarray ? @column_names : \@column_names;
}

1;

__END__

=head1 NAME

DataWarehouse::Dimension - The great new DataWarehouse::Dimension!

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';


=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use DataWarehouse::Dimension;

    my $foo = DataWarehouse::Dimension->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 SUBROUTINES/METHODS

=head2 function1

=cut

sub function1 {
}

=head2 function2

=cut

sub function2 {
}

=head1 AUTHOR

Nelson Ferraz, C<< <nferraz at gmail.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-dw at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DataWarehouse>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DataWarehouse::Dimension


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DataWarehouse>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/DataWarehouse>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/DataWarehouse>

=item * Search CPAN

L<http://search.cpan.org/dist/DataWarehouse/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2010 Nelson Ferraz.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.
