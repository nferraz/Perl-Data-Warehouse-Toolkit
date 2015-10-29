package DataWarehouse::Transform::Basic;

use warnings;
use strict;

sub head {
	my ($table,$n) = @_;

	my @rows = @$table; 
	@rows = splice(@rows,0,$n);		

	return \@rows;
}

sub tail {
	my ($table,$n) = @_;

	my $len = scalar @$table;

	my @rows = @$table; 
	@rows 	 = splice(@rows,$len-$n,$n);		

	return \@rows;
}

sub remove_column {
	my ($table,$field_idx) = @_;

	my @rows  = @$table;
	my $index = $field_idx - 1;

	my $i = 0;

	foreach my $row (@rows) {
		my (@cols) = @$row;
		splice(@cols,$index,1);

		$rows[$i++] = [ @cols ];	
	}

	return \@rows;
}

sub extract_columns {
	my ($table,$field_idx) = @_;

	my @rows  = @$table;

	my $i = 0;

	foreach my $row (@rows) {
		my (@cols)   = @$row;

		my @new_cols = map { $cols[$_-1] } @$field_idx; 
		$rows[$i++]  = [ @new_cols ];
	}

	return \@rows;
}

sub add_rownum {
	my $table = shift;
	
	my @rows  = @$table;
	my @new_rows;

	my $i = 0;

	foreach my $row (@rows) {
		my @cols = (@$row,++$i);
		push @new_rows, [ @cols ];	
	}

	return \@new_rows;
}

1;

__END__

=head1 NAME

DataWarehouse::Transform::Basic - module providing transformation operations on data sets

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

my $rows = [
                [ 1, 4, 6 ],
                [ 3, 2, 7 ],
           ];

my @head =  DataWarehouse::Transform::Basic::head($rows,1);

my @tail =  DataWarehouse::Transform::Basic::tail($rows,1);

my @rows =  DataWarehouse::Transform::Basic::remove_column($rows,2);

my @rows1 = DataWarehouse::Transform::Basic::extract_columns($rows,[2]);

my @rows2 = DataWarehouse::Transform::Basic::add_rownum($rows);

=head1 DESCRIPTION

This module provides various transformations to be performed on any data-set. 

=head1 FUNCTIONS

=over

=item 

head($rows,$n)

Parameters: Array reference of data-set matrix

Return value: Data table array of first n rows 

=cut

=item

tail($rows,$n)

Parameters: Array reference of data-set matrix 

Return value: Data table array of first n rows 

=cut

=item

remove_column($rows,$n)

Parameters: Array reference of data-set matrix, Field Index

Return value: Array of data table with n-th column removed 

=cut

=item

extract_columns($rows,$fields_idx_array_ref);

Parameters: Array reference of data-set matrix, Array reference of field indices 

Return value: Array of data table containing each of the fields corresponding to the indices 

=cut

=item

add_rownum($rows);

Parameters: Array reference of data-set matrix 

Return value: Array of data table with extra numeric auto-increment column 
=cut

=back

=head1 AUTHOR

Ashish Mukherjee (ashish.mukherjee@gmail.com) 
