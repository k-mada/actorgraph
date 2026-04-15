import type { PathStep } from '../../types';
import { useAppDispatch } from '../../context/hooks';
import { profileUrl, posterUrl } from '../../utils/images';

interface Props {
  path: PathStep[];
}

const ACTOR_IMG_CLS =
  'w-16 h-16 rounded-full object-cover bg-muted-surface flex items-center justify-center text-[22px] font-semibold text-muted-foreground';
const POSTER_CLS = 'w-12 h-18 rounded object-cover bg-muted-surface';
const CARD_CLS =
  'flex flex-col items-center gap-1.5 p-2 rounded-lg hover:bg-muted-surface transition-colors';
const LABEL_CLS =
  'text-xs text-center max-w-20 overflow-hidden text-ellipsis whitespace-nowrap';

export function PathChain({ path }: Props) {
  const dispatch = useAppDispatch();

  return (
    <div className="flex items-center overflow-x-auto py-4">
      {path.map((step, i) => (
        <div key={i} className="flex items-center shrink-0">
          {i > 0 && (
            <span className="text-xl text-muted-foreground mx-2">&rarr;</span>
          )}
          {step.type === 'actor' ? (
            <button
              className={CARD_CLS}
              onClick={() =>
                dispatch({
                  type: 'OPEN_DETAIL',
                  detail: { type: 'actor', tmdb_id: step.tmdb_id },
                })
              }
            >
              {step.profile_path ? (
                <img
                  className={ACTOR_IMG_CLS}
                  src={profileUrl(step.profile_path, 'w185')!}
                  alt=""
                />
              ) : (
                <div className={ACTOR_IMG_CLS}>
                  <span>{step.name[0]}</span>
                </div>
              )}
              <span className={LABEL_CLS}>{step.name}</span>
            </button>
          ) : (
            <button
              className={CARD_CLS}
              onClick={() =>
                dispatch({
                  type: 'OPEN_DETAIL',
                  detail: { type: 'movie', tmdb_id: step.tmdb_id },
                })
              }
            >
              {(step.poster_url || step.poster_path) ? (
                <img
                  className={POSTER_CLS}
                  src={(step.poster_url ?? posterUrl(step.poster_path, 'w154'))!}
                  alt=""
                />
              ) : (
                <div className={POSTER_CLS} />
              )}
              <span className={LABEL_CLS}>{step.title}</span>
            </button>
          )}
        </div>
      ))}
    </div>
  );
}
