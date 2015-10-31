#!/usr/bin/env perl

use strict;
use warnings;

use Test::More;
use Data::Dumper;

plan tests => 6;

use DataWarehouse::Transform::Basic;
use DataWarehouse::DataSet;

my $data_rows  = [ [ 1 .. 3 ], [ 4 .. 6 ], [ 7 .. 9 ], [ 10 .. 12 ] ];

subtest 'head' => sub {
	my @test_table = (
			new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] }),
			new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] })
			);

    my @tests = (
        {
            label  => 'head with n = 1',
            table  => $test_table[0],
            n      => 1,
            expect => [ [ 1 .. 3 ] ],
        },
        {
            label  => 'head with n = 2',
            table  => $test_table[1],
            n      => 2,
            expect => [ [ 1 .. 3 ], [ 4 .. 6 ] ],
        },

    );

    plan tests => scalar @tests;

    for my $test (@tests) {
        my $got =
          DataWarehouse::Transform::Basic::head( $test->{table}, $test->{n} )->get_data_rows();

        is_deeply( $got, $test->{expect}, $test->{label} )
          or diag( Dumper $got);
    }
};

subtest 'tail' => sub {
	my @test_table = (
			new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] }),
			new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] })
			);

    my @tests = (
        {
            label  => 'tail with n = 1',
            table  => $test_table[0],
            n      => 1,
            expect => [ [ 10 .. 12 ] ],
        },
        {
            label  => 'tail with n = 2',
            table  => $test_table[1],
            n      => 2,
            expect => [ [ 7 .. 9 ], [ 10 .. 12 ] ],
        },

    );

    plan tests => scalar @tests;

    for my $test (@tests) {
        my $got =
          DataWarehouse::Transform::Basic::tail( $test->{table}, $test->{n} )->get_data_rows();
        is_deeply( $got, $test->{expect}, $test->{label} )
          or diag( Dumper $got);
    }
};

subtest 'remove_column' => sub {
	my @test_table = (
			new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] }),
			new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] }),
			new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] })
			);

    my @tests = (
        {
            label     => 'remove_column with field_idx = 1',
            table     => $test_table[0],
            field_idx => 1,
            expect    => [ [ 2, 3 ], [ 5, 6 ], [ 8, 9 ], [ 11, 12 ] ],
        },
        {
            label     => 'remove_column with field_idx = 2',
            table     => $test_table[1],
            field_idx => 2,
            expect    => [ [ 1, 3 ], [ 4, 6 ], [ 7, 9 ], [ 10, 12 ] ],
        },
        {
            label     => 'remove_column with field_idx = 3',
            table     => $test_table[2],
            field_idx => 3,
            expect    => [ [ 1, 2 ], [ 4, 5 ], [ 7, 8 ], [ 10, 11 ] ],
        },
    );

    plan tests => scalar @tests;

    for my $test (@tests) {
        my $got =
          DataWarehouse::Transform::Basic::remove_column( $test->{table},
            $test->{field_idx} )->get_data_rows();
        is_deeply( $got, $test->{expect}, $test->{label} )
          or diag( Dumper $got);
    }
};


subtest 'extract_columns' => sub {
	my @test_table = (
			new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] }),
			new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] }),
			new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] }),
			new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] })
			);

    my @tests = (
        {
            label     => 'extract_column with field_idx = [1]',
            table     => $test_table[0],
            field_idx => [1],
            expect    => [ [1], [4], [7], [10] ],
        },
        {
            label     => 'extract_column with field_idx = [2]',
            table     => $test_table[1],
            field_idx => [2],
            expect    => [ [2], [5], [8], [11] ],
        },
        {
            label     => 'extract_column with field_idx = [3]',
            table     => $test_table[2],
            field_idx => [3],
            expect    => [ [3], [6], [9], [12] ],
        },
        {
            label     => 'extract_column with field_idx = [1,3]',
            table     => $test_table[3],
            field_idx => [ 1, 3 ],
            expect    => [ [ 1, 3 ], [ 4, 6 ], [ 7, 9 ], [ 10, 12 ] ],
        },
    );

    plan tests => scalar @tests;

    for my $test (@tests) {
        my $got =
          DataWarehouse::Transform::Basic::extract_columns( $test->{table},
            $test->{field_idx} )->get_data_rows();
        is_deeply( $got, $test->{expect}, $test->{label} )
          or diag( Dumper $got);
    }
};

subtest 'add_rownum' => sub {
	my $test_table = new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] });

    my @tests = (
        {
            label => 'extract_column with field_idx = [1]',
            table => $test_table,
            expect =>
              [ [ 1 .. 3, 1 ], [ 4 .. 6, 2 ], [ 7 .. 9, 3 ], [ 10 .. 12, 4 ] ],
        },
    );

    plan tests => scalar @tests;

    for my $test (@tests) {
        my $got =
          DataWarehouse::Transform::Basic::add_rownum( $test->{table},
            $test->{field_idx} )->get_data_rows();
        is_deeply( $got, $test->{expect}, $test->{label} )
          or diag( Dumper $got);
    }
};

subtest 'add_computed_field' => sub {
	my $sub_ref = sub {
		my $_row = shift;
		$_row->[3] = $_row->[0] + $_row->[1] + $_row->[2];
	};

	my $test_table = new DataWarehouse::DataSet({ rows => $data_rows, headers => [ 'h1','h2','h3' ] });

    my @tests = (
        {
            label => 'modify rows',
            table => $test_table,
            expect =>
              [ [ 1 .. 3, 6 ], [ 4 .. 6, 15 ], [ 7 .. 9, 24 ], [ 10 .. 12, 33 ] ],
        },
    );

    plan tests => scalar @tests;

    for my $test (@tests) {
        my $got =
          DataWarehouse::Transform::Basic::add_computed_field( $test->{table},
            $sub_ref, "Computed Field 1" )->get_data_rows();

        is_deeply( $got, $test->{expect}, $test->{label} )
          or diag( Dumper $got);
    }
};
