package VSGDR::StaticData;

use strict;
use warnings;

use 5.010;

use Scalar::Util qw(looks_like_number);
use POSIX qw(strftime);
use Carp;
use DBI;
use Data::Dumper;


=head1 NAME

VSGDR::StaticData - Static data script support package for SSDT post-deployment steps, Ded MedVed..

=head1 VERSION

Version 0.02

=cut

our $VERSION = '0.02';


1;


sub databaseName {

    local $_    = undef ;

    my $dbh     = shift ;

    my $sth2 = $dbh->prepare(databaseNameSQL());
    my $rs   = $sth2->execute();
    my $res  = $sth2->fetchall_arrayref() ;

    return $$res[0][0] ;

}

sub databaseNameSQL {

return <<"EOF" ;

select  db_name()

EOF

}

sub dependency {

    local $_    = undef ;

    my $dbh     = shift ;

    my $sth2    = $dbh->prepare( dependencySQL());
    my $rs      = $sth2->execute();
    my $res     = $sth2->fetchall_arrayref() ;

    if ( scalar @{$res} ) { return $res ; } ;
    return [] ;
}



sub dependencySQL {

return <<"EOF" ;
select  distinct
        tc2.TABLE_CATALOG               as to_CATALOG
,       tc2.TABLE_SCHEMA                as to_SCHEMA 
,       tc2.TABLE_NAME                  as to_NAME   
,       tc1.TABLE_CATALOG               as from_CATALOG
,       tc1.TABLE_SCHEMA                as from_SCHEMA
,       tc1.TABLE_NAME                  as from_NAME
from    INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
join    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc1
on      tc1.CONSTRAINT_SCHEMA           = rc.CONSTRAINT_SCHEMA
and     tc1.CONSTRAINT_CATALOG          = rc.CONSTRAINT_CATALOG
and     tc1.CONSTRAINT_NAME             = rc.CONSTRAINT_NAME
join    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc2
on      tc2.CONSTRAINT_SCHEMA           = rc.CONSTRAINT_SCHEMA
and     tc2.CONSTRAINT_CATALOG          = rc.CONSTRAINT_CATALOG
and     tc2.CONSTRAINT_NAME             = rc.UNIQUE_CONSTRAINT_NAME

EOF

}


sub generateScript {

    local $_ = undef;

    my $dbh     = shift ;
    my $schema  = shift ;
    my $table   = shift ;

    croak "bad arg dbh"     unless defined $dbh;
    croak "bad arg schema"  unless defined $schema;
    croak "bad arg table"   unless defined $table;

    my $combinedName    = "${schema}.${table}"; 
warn Dumper $dbh;    
    my $database        = databaseName($dbh);

    no warnings;
    my $userName        = @{[getpwuid( $< )]}->[6]; $userName =~ s/,.*//;
    use warnings;
    my $date            = strftime "%d/%m/%Y", localtime;



    my $hasId                   = has_idCols($dbh,$schema,$table) ;
    my $set_IDENTITY_INSERT_ON  = "";
    my $set_IDENTITY_INSERT_OFF = "";
    $set_IDENTITY_INSERT_ON     = "set IDENTITY_INSERT ${combinedName} ON"  if $hasId;
    $set_IDENTITY_INSERT_OFF    = "set IDENTITY_INSERT ${combinedName} OFF" if $hasId;


    my $ra_columns              = columns($dbh,$schema,$table);
    my $ra_pkcolumns            = pkcolumns($dbh,$schema,$table);

    croak 'No Primary Key defined'          unless scalar @{$ra_pkcolumns};
    croak 'Unusable Primary Key defined'    unless scalar @{$ra_pkcolumns} == 1;

    my $pk_column = $ra_pkcolumns->[0][0];    
    my @nonKeyColumns = grep { $_->[0] ne $pk_column } @{$ra_columns};


    my $variabledeclaration     = "declare\t" ;
    my $tabledeclaration        = "\t(\tId\t\tint\tnot null\n\t\t,\t" ;
    my $selectstatement         = "select\t" ;
    my $insertclause            = "insert into ${combinedName}\n\t\t\t\t(";
    my $valuesclause            = "values(";
    my $flatcolumnlist          = "" ;
    my $flatvariablelist        = "" ;
    my $updateColumns           = "set\t";
    my $printStatement          = "" ;

    foreach my $l (@{$ra_columns}) {
        do { local $" = "\t"; $variabledeclaration      .= "@"."@{$l}[0,1,2,3,5]" ; $variabledeclaration .= "\n\t,\t\t"} ;
        do { local $" = "\t"; $tabledeclaration         .= "@{$l}" ; $tabledeclaration .= "\n\t\t,\t"} ;
        do { local $" = "";   $selectstatement          .= "@"."$l->[0]\t\t= $l->[0]" ; $selectstatement .= "\n\t\t,\t\t"} ;
        do { local $" = "";   $insertclause             .= "$l->[0]" ; $insertclause .= ", "} ;    
        do { local $" = "";   $valuesclause             .= "$l->[0]" ; $valuesclause .= ", "} ;    
        do { local $" = "";   $flatcolumnlist           .= "$l->[0]" ; $flatcolumnlist .= ", "} ;
        do { local $" = "";   $flatvariablelist         .= "@"."$l->[0]" ; $flatvariablelist .= ", "} ;

        do { local $" = "";   $printStatement           .= "'  $$l[0]: ' " ; } ;
        my $printFragment                               = $$l[1] !~ m{ (?: char ) }ixms 
                                                                  ? "cast( \@$$l[0] as varchar(128))"
                                                                  : "\@$$l[0]" ; 

        $printFragment   = " + case when ${printFragment} is null then 'NULL' else '''' + ${printFragment} + '''' end + " ;                                                                  ;
        $printStatement .= $printFragment ;
    }
    foreach my $l (@nonKeyColumns) {
        do { local $" = "";   $updateColumns            .= "$l->[0]\t\t= "."@"."$l->[0]" ; $updateColumns .= "\n\t\t\t\t\t,\t"} ;
    }

    # trim off erroneous trailing cruft - better to resign array interpolations above .
    $variabledeclaration      =~ s{ \n\t,\t\t \z }{}msx;
    $tabledeclaration         =~ s{ \n\t\t,\t \z }{}msx;
    $selectstatement          =~ s{ \n\t\t,\t\t \z }{}msx;
    $updateColumns            =~ s{ \n\t\t\t,\t \z }{}msx;
    $insertclause             =~ s{ ,\s \z }{}msx;
    $valuesclause             =~ s{ ,\s \z }{}msx;
    $flatcolumnlist           =~ s{ ,\s \z }{}msx;
    $flatvariablelist         =~ s{ ,\s \z }{}msx;
    $updateColumns            =~ s{ \n\t\t\t\t\t,\t \z }{}msx;
    $printStatement           =~ s{ \+\s \z }{}msx;


    $tabledeclaration   .= "\n\t\t)";
    $insertclause       .= ")";
    $valuesclause       .= ")";

    my $insertingPrintStatement = "'Inserting ${combinedName}:' + " . $printStatement ;
    my $updatingPrintStatement  = "'Updating ${combinedName}: ' + " . $printStatement;
    my $noopPrintStatement      = "'Nothing to update. ${combinedName}: Values are unchanged for Primary Key: '";


    my $ra_data = getCurrentTableData($dbh,$combinedName,$pk_column);

    my @valuesTable     = undef;
    my $valuesClause    = "values\n\t\t";

    my $lno             = 1;
    foreach my $ra_row (@{$ra_data}){
    #    warn Dumper $ra_row;
        my @outVals = map { looks_like_number($_) ? $_ : $dbh->quote($_)  } @{$ra_row};
        my $line = do{ local $" = ", "; "@outVals" } ;
        $valuesClause    .= "(\t" . "$lno, " . $line . ")" . "\n\t\t,\t" ;
        $lno++;
    }
    $valuesClause        =~ s{ \n\t\t,\t \z }{}msx;

return <<"EOF";

/****************************************************************************************
 * Database:    ${database}
 * Author  :    ${userName}
 * Date    :    ${date}
 * Purpose :    Static data deployment script for ${combinedName}
 *              
 *
 * Version History
 * ---------------
 * 1.0.0    ${date} ${userName}
 * Created.
 ***************************************************************************************/  

set nocount on 

declare  \@DeployCmd                varchar(20)     
        ,\@DeploySwitch             bit             

set     \@DeployCmd                 = '\$(StaticDataDeploy)'
set     \@DeploySwitch              = 0
--Check whether a deploy has been stated.
if isnull(upper(\@DeployCmd) , '') <> 'DEPLOY'
    begin
        set \@DeploySwitch = 0 --FALSE, only run a dummy deploy where no actual data will be modified.
        print 'Deploy Type: Dummy Deploy (No data will be changed)'
    end
else
    begin
        set \@DeploySwitch = 1 --TRUE, run real deploy.
        print 'Deploy Type: Actual Deploy'
    end



begin try

    -- Declarations
    declare \@ct                         int          
    ,       \@i                          int          

    declare \@localTransactionStarted bit;


    begin transaction
    save transaction sc_Config_${table}_SavePoint;

    set \@localTransactionStarted = 1;

    declare \@${table} table
    ${tabledeclaration}
    
    ; with src as 
    (
    select * 
    from(  ${valuesClause}
        ) AS vtable 
    ( Id, $flatcolumnlist)
    )
    insert  into
            \@${table} 
    (       Id,             ${flatcolumnlist}
    )
    select  Id,   ${flatcolumnlist}
    from    src

    ${variabledeclaration}        




    -- count how many records need to be inserted
    select \@ct = count(*) from \@${table}

    set \@i = 1
    -- insert the records into the ${table} table if they don't already exist, otherwise update them
    while \@i <=\@ct begin

        ${selectstatement}
        from    \@${table}
        where   Id = \@i

        if not exists
                (
                select  * 
                from    $combinedName
                where   ${pk_column}        = \@${pk_column}
                )  begin
            print $insertingPrintStatement
            if \@DeploySwitch = 1 begin
                ${set_IDENTITY_INSERT_ON}
                ${insertclause}
                values (${flatvariablelist})
                ${set_IDENTITY_INSERT_OFF}
            end
        end
        -- if the static data doesn't match what is already there then update it.
        -- 'except' handily handles null as equal.  Saves some extensive twisted logic.
        else begin
            if exists 
                (
                select  ${flatcolumnlist}
                from    $combinedName
                where   ${pk_column}    = \@${pk_column}
                except
                select  ${flatvariablelist}
                ) begin
                print $updatingPrintStatement
                if \@DeploySwitch = 1 begin
                    update  s
                    ${updateColumns}
                    from    $combinedName s
                    where   ${pk_column}        = \@${pk_column}
                end
            end
            else  begin
                print $noopPrintStatement + cast(\@${pk_column} as varchar(10)) 
            end 
        end

        set \@i=\@i+1
    end

    commit



end try
begin catch

    print error_message() 
    -- Rollback any locally begun transaction.  Don't fail the whole deployment if it's transactional.
    -- If our transaction is the only one, then just rollback.
    if \@\@trancount > 0 begin
        if \@\@trancount > 1 begin
            if \@localTransactionStarted is not null and \@localTransactionStarted = 1 
                rollback transaction sc_Config_${table}_SavePoint;
                commit ; --  windback our local transaction completely
            --else it's probably nothing to do with us.
        end
            else rollback
    end

    print 'Deployment of ${table} static data for NSI developer deployment failed .'
    print error_message()
    
    ${set_IDENTITY_INSERT_OFF}


end catch

go


EOF

}


sub getCurrentTableData {

    local $_ = undef ;
    
    my $dbh          = shift or croak 'no dbh' ;
    my $combinedName = shift or croak 'no table' ;
    my $pkCol        = shift or croak 'no primary key' ;

    my $sth2 = $dbh->prepare(getCurrentTableDataSQL($combinedName,$pkCol));
    my $rs   = $sth2->execute();
    my $res  = $sth2->fetchall_arrayref() ;

    return $res ;

}

sub getCurrentTableDataSQL {

    local $_ = undef ;
    
    my $combinedName = shift or croak 'no table' ;
    my $pkCol        = shift or croak 'no primary key' ;

return <<"EOF" ;

select  *
from    $combinedName so
order   by
        $pkCol
EOF

}


sub has_idCols {

    local $_ = undef ;
    
    my $dbh          = shift or croak 'no dbh' ;
    my $schema = shift or croak 'no schema' ;
    my $table = shift or croak 'no table' ;

    my $sth2 = $dbh->prepare(has_idColsSQL());
    my $rs   = $sth2->execute($schema,$table);
    my $res  = $sth2->fetchall_arrayref() ;

    return $$res[0][0] ;

}

sub has_idColsSQL {

return <<"EOF" ;

select  1 as ID_COL
FROM    dbo.sysobjects so
where   user_name(so.uid)   = ?
and     so.name             = ?
and     exists (
        select *
        from dbo.syscolumns sc
        where so.id = sc.id
        and   sc.colstat & 1 = 1
        )
EOF

}


sub pkcolumns {

    local $_    = undef ;
    
    my $dbh     = shift or croak 'no dbh' ;
    my $schema  = shift or croak 'no schema' ;
    my $table   = shift or croak 'no table' ;

    my $sth2    = $dbh->prepare( pkcolumnsSQL());
    my $rs      = $sth2->execute($schema,$table);
    my $res     = $sth2->fetchall_arrayref() ;

    if ( scalar @{$res} ) { return $res ; } ;
    return [] ;
}



sub pkcolumnsSQL {

return <<"EOF" ;
select  COLUMN_NAME 
from    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
join    INFORMATION_SCHEMA.KEY_COLUMN_USAGE  kcu
on      tc.TABLE_CATALOG            = kcu.TABLE_CATALOG
and     tc.TABLE_SCHEMA             = kcu.TABLE_SCHEMA
and     tc.TABLE_NAME               = kcu.TABLE_NAME
and     tc.CONSTRAINT_NAME          = kcu.CONSTRAINT_NAME
where   tc.CONSTRAINT_TYPE          = 'PRIMARY KEY'
and     tc.TABLE_SCHEMA             = ?
and     tc.TABLE_NAME               = ?
order   by ORDINAL_POSITION

EOF

}


sub columns {

    local $_    = undef ;

    my $dbh          = shift or croak 'no dbh' ;
    my $schema  = shift or croak 'no schema' ;
    my $table   = shift or croak 'no table' ;

    my $sth2    = $dbh->prepare( columnsSQL());
    my $rs      = $sth2->execute($schema,$table);
    my $res     = $sth2->fetchall_arrayref() ;

    if ( scalar @{$res} ) { return $res ; } ;
    return [] ;
}



sub columnsSQL {

return <<"EOF" ;
select  Column_name 
,       data_type
,       case when character_maximum_length is not null then '('+cast(character_maximum_length as varchar(10))+')' else '' end 
        as datasize
,       case when lower(Data_type) not like '%int%' and Numeric_precision is not null then '('+cast(Numeric_precision as varchar(10))+','+cast(Numeric_scale as varchar(10))+')' else '' end 
        as dataprecision
,       case when LOWER(IS_NULLABLE) = 'no' then 'not null' else 'null' end
        as datanullabity
,       case when DATABASEPROPERTYEX(db_name(), 'Collation') != collation_name then collation_name else '' end 
        as collation
from    INFORMATION_SCHEMA.COLUMNS
where   1=1
and     TABLE_SCHEMA        = ?
and     TABLE_NAME          = ?
and     COLUMNPROPERTY(object_id(TABLE_NAME) , COLUMN_NAME,'IsComputed') != 1
EOF

}






__DATA__



=head1 SYNOPSIS

Package to support the generation of static data population scripts for SQL Server Data Tools post-deployment steps.

=head1 AUTHOR

Ded MedVed, C<< <dedmedved@cpan.org> >>


=head1 BUGS


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc VSGDR::StaticData


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2013 Ded MedVed.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1; # End of StaticData
