from typing import Optional, Dict

from requests import RequestException

from redash_client import Dashboard, DataSource, User, Group, Widget, Query, DataSourceOptions

DEFAULT_GROUP_PERMISSIONS = '{create_dashboard,create_query,edit_dashboard,edit_query,view_query,view_source,' \
                            'execute_query,list_users,schedule_query,list_dashboards,list_alerts,list_data_sources}'
MERCHANT_GROUP_PERMISSIONS = '{edit_dashboard,view_query,execute_query,schedule_query,list_dashboards,list_alerts,' \
                             'list_data_sources}'
DASHBOARD_PREFIX = 'Dashboard - '
SHOP_ID_SUFFIX = ' ({})'
ARCHIVE_OLD_QUERIES_AND_DASHBOARDS_SQL = """
update queries set is_archived=TRUE where data_source_id is null and is_archived=FALSE;
update dashboards set is_archived=TRUE where id in (
    select distinct (dashboards.id)
    from dashboards
          join widgets w on dashboards.id = w.dashboard_id
          join visualizations v on w.visualization_id = v.id
          join queries q on q.id = v.query_id
    where dashboards.is_archived=FALSE and q.is_archived = true);
"""


class GenerateDashboard:
    """
    This script clones a dashboard and its queries, while using a new data source.
    It assigns the provided users to a group with access only to that data source,
    and changes the shop_id and name in the new cloned queries.
    """
    shop_id: int = 0
    shop_name: str = ''

    # Provide either dashboard_template_slug_name or (dashboard_template_shop_name and dashboard_template_shop_id)
    dashboard_template_slug_name: str = ''
    dashboard_template_shop_name: str = ''
    dashboard_template_shop_id: int = 0

    users_data: Optional[str] = ''  # user_email_1,user_name_1;user_email_2,user_name_2;...
    redash_postgres_client = None  # TODO: Need to use an open source postgres client

    def __init__(self):
        self.queries_cache: Dict[int, Query] = {}
        self.data_sources_cache: Dict[int, DataSource] = {}
        self.new_data_source_id = None

    @staticmethod
    def print_log(msg: str):
        print(f'[#]  {msg}')

    def run(self):
        if not self.dashboard_template_slug_name and not self.dashboard_template_shop_name:
            raise ValueError('Both template slug name and template shop name are missing, at least one is required!')

        self.print_log(f'Generating dashboard for {self.shop_name} ({self.shop_id})')
        self.generate_data_source_group_and_users()
        self.clone_dashboard()
        self.archive_old_queries_and_dashboards()
        self.print_log(f'Finished generating dashboard!')

    def generate_data_source_group_and_users(self):
        """Generate a new data source (redshift), a new group for specific permission, and new users (optional) who will
        get access to the new data source only
        """
        # Create users and set permissions only to the new group
        new_users_list = []
        existing_users = []
        if self.users_data:
            for user_data in self.users_data.split(";"):
                email, name = [field.strip() for field in user_data.split(',')]
                user, created = self.get_or_create_user(email, name)
                (new_users_list if created else existing_users).append(user)

        # Create group and data source
        formatted_name = self.shop_name + SHOP_ID_SUFFIX.format(self.shop_id)
        group = self.get_or_create_group(formatted_name)
        self.set_group_permissions(group.id, MERCHANT_GROUP_PERMISSIONS)

        redshift_data_source_id = self.create_data_source(formatted_name).id

        # Add data source to group
        group.add_data_source(redshift_data_source_id)
        group.set_data_source_access(redshift_data_source_id)

        self.print_log(f'Added data source to group')

        # Add existing users to the group
        for existing_user in existing_users:
            if group.id in existing_user.group_ids:
                self.print_log(f'Existing user is already part of the group. user_id: {existing_user.id}')
            else:
                existing_user.group_ids.append(group.id)
                existing_user.save()
                self.print_log(f'Added existing user to group. user_id: {existing_user.id}')

        # Add new users to the group
        for new_user in new_users_list:
            new_user.group_ids = [group.id]
            new_user.save()
            self.print_log(f'Set user group. user_id: {new_user.id}')

        self.new_data_source_id = redshift_data_source_id

    def get_or_create_user(self, email: str, name: str) -> (User, bool):
        """
        Get an existing user or creates a new one.
        Returns a tuple of (user, created)
        """
        new_user = User(name=name, email=email)
        try:
            new_user.save()
            self.print_log(f'Created new user. id: {new_user.id}')
            return new_user, True
        except RequestException as e:
            if e.response is not None and e.response.status_code == 400:
                for existing_user in User.objects(q=email):
                    if existing_user.email == email:
                        self.print_log(f'Found existing user. id: {existing_user.id}')
                        return existing_user, False

            self.print_log(f'Failed get or create user. {email=} {name=}')
            raise

    def get_or_create_group(self, group_name: str) -> Group:
        for group in Group.objects():
            if group.name == group_name:
                self.print_log(f'Found existing group. id: {group.id}')
                return group

        group = Group(name=group_name)
        group.save()
        self.print_log(f'Created new group. id: {group.id}')
        return group

    def create_data_source(self, data_source_name: str, options: DataSourceOptions = None) -> DataSource:
        """
        If there is already a data source with that name, we delete it so that all the related queries will be
        deleted as well
        @param data_source_name:
        @param options: credentials for the data_source
        @return:
        """
        new_data_source = DataSource(name=data_source_name)
        if options:
            new_data_source.options = options

        new_data_source.save()
        self.print_log(f'Created new data source (id: {new_data_source.id})')

        for data_source in DataSource.objects():
            if data_source.id != new_data_source.id and data_source.name == data_source_name:
                data_source.delete()
                self.print_log(f'Deleted duplicate data source (id: {data_source.id})')

        return new_data_source

    def set_group_permissions(self, group_id, permissions=DEFAULT_GROUP_PERMISSIONS):
        self.redash_postgres_client.execute_only(f"UPDATE groups SET permissions='{permissions}' where id={group_id}")

    def clone_dashboard(self):
        if self.dashboard_template_slug_name:
            original_dashboard = Dashboard.get(self.dashboard_template_slug_name)
            if not self.dashboard_template_shop_name and original_dashboard:
                self.dashboard_template_shop_name = original_dashboard.name.split(DASHBOARD_PREFIX)[1]
                self.print_log(f'Dashboard template shop name was not provided. '
                               f'Parsed shop name from dashboard. shop_name: {self.dashboard_template_shop_name}')
        else:
            original_dashboard = next(
                Dashboard.objects(q=DASHBOARD_PREFIX + self.dashboard_template_shop_name), None)

            if not original_dashboard:
                raise ValueError(f'Can\'t find template dashboard by name: {self.dashboard_template_shop_name}')

            original_dashboard.fetch()
            self.dashboard_template_slug_name = original_dashboard.slug
            self.print_log(f'Dashboard template slug was not provided. '
                           f'Found dashboard by shop name. slug: {self.dashboard_template_slug_name}')

        if not original_dashboard:
            raise ValueError(f'Can\'t find template dashboard: {self.dashboard_template_slug_name}')

        new_dashboard = Dashboard(
            name=original_dashboard.name.replace(self.dashboard_template_shop_name, self.shop_name, 1))

        new_dashboard.save()
        self.print_log(f'Created new dashboard. id: {new_dashboard.id}, slug: {new_dashboard.slug}\n')

        for widget in original_dashboard.widgets:
            self.clone_widget(widget, new_dashboard.id)

        new_dashboard.dashboard_filters_enabled = True
        new_dashboard.is_draft = False
        new_dashboard.save()

    def clone_widget(self, widget: Widget, new_dashboard_id: int):
        self.print_log(
            f'Cloning widget. id: {widget.id}, name: {widget.visualization.query.name} - {widget.visualization.name}')
        original_visualization = widget.visualization
        original_query = original_visualization.query
        original_query.fetch()

        new_query = self.get_or_create_new_query(original_query)

        for visualization in new_query.visualizations:
            if visualization.name == original_visualization.name and \
                    visualization.type == original_visualization.type and \
                    visualization.options == original_visualization.options:
                widget.visualization_id = visualization.id
                break
        else:
            raise Exception('Could not find matching visualization on new query')

        widget.visualization = None
        widget.dashboard_id = new_dashboard_id
        widget.id = None
        widget.save()
        self.print_log(f'Created new widget. id: {widget.id}\n')

    def get_or_create_new_query(self, original_query: Query) -> Query:
        new_query = self.queries_cache.get(original_query.id)
        if not new_query:
            new_query = original_query.fork()
            self.print_log(f'Forked query. existing_id: {original_query.id}, new_id: {new_query.id}')
            new_query.name = original_query.name.replace(self.dashboard_template_shop_name, self.shop_name, 1)

            # Replace shop ids to the target shop ids
            new_query.query = original_query.query \
                .replace(str(self.dashboard_template_shop_id), str(self.shop_id))

            new_query.data_source_id = self.new_data_source_id

            new_query.is_draft = False
            new_query.save()
            self.queries_cache[original_query.id] = new_query

        return new_query

    def archive_old_queries_and_dashboards(self):
        self.print_log('Archiving old queries and dashboards')
        self.redash_postgres_client.execute_only(ARCHIVE_OLD_QUERIES_AND_DASHBOARDS_SQL)
