create sequence dataset_map_system_id_seq;
create table public.dataset_map (
                                    system_id bigint primary key not null default nextval('dataset_map_system_id_seq'::regclass),
                                    next_counter_value bigint not null,
                                    locale_name character varying(40) not null,
                                    obfuscation_key bytea not null,
                                    resource_name character varying(128),
                                    latest_data_version bigint not null default 0
);
create index dataset_map_resource_name on dataset_map using btree (resource_name);

create sequence copy_map_system_id_seq;
create table public.copy_map (
                                 system_id bigint primary key not null default nextval('copy_map_system_id_seq'::regclass),
                                 dataset_system_id bigint not null,
                                 copy_number bigint not null,
                                 lifecycle_stage dataset_lifecycle_stage not null,
                                 data_version bigint not null,
                                 last_modified timestamp with time zone not null default CURRENT_TIMESTAMP,
                                 data_shape_version bigint,
                                 foreign key (dataset_system_id) references public.dataset_map (system_id)
                                     match simple on update no action on delete no action
);
create unique index copy_map_dataset_system_id_copy_number_key on copy_map using btree (dataset_system_id, copy_number);

create table public.column_map (
                                   system_id bigint not null,
                                   copy_system_id bigint not null,
                                   user_column_id character varying(40) not null,
                                   type_name character varying(40) not null,
                                   physical_column_base_base character varying(40) not null,
                                   is_system_primary_key unit,
                                   is_user_primary_key unit,
                                   is_version unit,
                                   field_name text,
                                   field_name_casefolded text,
                                   primary key (system_id, copy_system_id),
                                   foreign key (copy_system_id) references public.copy_map (system_id)
                                       match simple on update no action on delete no action
);
create unique index column_map_copy_system_id_user_column_id_key on column_map using btree (copy_system_id, user_column_id);
create unique index column_map_copy_system_id_is_system_primary_key_key on column_map using btree (copy_system_id, is_system_primary_key);
create unique index column_map_copy_system_id_is_user_primary_key_key on column_map using btree (copy_system_id, is_user_primary_key);
create unique index column_map_copy_system_id_is_version_key on column_map using btree (copy_system_id, is_version);
create unique index column_map_copy_system_id_field_name_casefolded_key on column_map using btree (copy_system_id, field_name_casefolded);
